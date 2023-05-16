use std::error::Error;
use std::collections::HashMap;
use tokio::sync::mpsc;
use log::*;
use feed_fetcher::{read_plan, url_to_host};
use clap::Parser;
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Serialize)]
struct FetchEntry<'a> {
    url: &'a str,
    title: &'a str,
    published: &'a str,
    seen: &'a str,
    downloaded: &'a str,
    feed: &'a str,
    body: &'a str,
}

const VERSION: &str = git_version::git_version!(args=["--tags","--always", "--dirty"]);

/// Update fetch plan from a list of feeds
#[derive(Parser, Debug)]
#[clap(author, version=VERSION, about)]
struct Args {
    /// processing plan
    planfile: String,

    /// jsonlines fetched data output file
    data_out: String,

    /// time in seconds, after which I will give up
    #[arg(long)]
    time_limit: Option<u64>,

    /// compress the output with zstd
    #[arg(long)]
    compress: Option<bool>,

    /// append, do not truncate the output file
    #[arg(long)]
    append: Option<bool>,

    /// HTTP client timeout
    #[arg(long,default_value_t=20)]
    http_timeout: u64,

    /// HTTP client timeout for the connection phase only
    #[arg(long,default_value_t=12)]
    http_connect_timeout: u64,

    /// delay between successive HTTP request to the same server
    #[arg(long,default_value_t=5)]
    wait: u64,
}

async fn fetch_page(client: &reqwest::Client, page_url: &str) ->
        Result<String, String> {
    let response = match client.get(page_url).send().await {
        Ok(response) => response,
        Err(e) => {
            warn!("GET for {} failed: {}", page_url, e);
            return Err("get".to_string());
        },
    };
    if response.status() != 200 {
        return Err(format!("{}", response.status()));
    }
    let body = match response.text().await {
        Ok(body) => body,
        Err(e) => {
            warn!("response for {} failed: {}", page_url, e);
            return Err("res".to_string());
        },
    };
    Ok(body)
}

async fn fetch_pages_single_origin(
        client: reqwest::Client, url_with_id: Vec<(usize, String)>,
        tx: tokio::sync::mpsc::Sender<(usize, String, Result<String, String>)>,
        mut terminate_rx: tokio::sync::broadcast::Receiver<bool>, wait: u64) ->
            Result<(), Box<dyn Error + Sync + Send>> {
    let mut first = true;
    for (planidx, page_url) in url_with_id {
        match terminate_rx.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {},
            _ => break,
        }
        if first { first = false; } else {
            tokio::time::sleep(tokio::time::Duration::from_secs(wait)).await
        }
        let res = fetch_page(&client, &page_url).await;
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        tx.send((planidx, ts, res)).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")).init();

    info!("opening data output at {}", &args.data_out);
    let data_outf = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(args.append.unwrap_or(false))
        .truncate(!args.append.unwrap_or(false))
        .open(&args.data_out)?;
    let data_wr: Box<dyn std::io::Write> = match args.compress {
        Some(true) => Box::new(zstd::Encoder::new(&data_outf, 0)?),
        _ => Box::new(&data_outf),
    };
    let mut data_out_json = serde_jsonlines::JsonLinesWriter::new(data_wr);

    info!("reading plan from {}", &args.planfile);
    let mut plan = read_plan(&args.planfile)?;
    info!("there are {} plan elements in total", plan.len());

    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let planfile_bkp = args.planfile.to_string() + "." + &ts + ".fetch.bkp";
    info!("moving plan to {}", &planfile_bkp);
    std::fs::rename(&args.planfile, planfile_bkp)?;

    let mut plan_out = std::fs::File::create(&args.planfile)?;

    info!("preparing fetch lists");
    let mut pages_all = 0;
    let mut entries_by_host = HashMap::<String, Vec<(usize, String)>>::new();
    for (planidx, entry) in plan.iter().enumerate() {
        if entry.retries > 4 { continue; };
        if entry.status == "ok" { continue; };
        match url_to_host(&entry.url) {
            Ok(host) => {
                entries_by_host.entry(host)
                .or_insert_with(Vec::new).push((planidx, entry.url.to_string()));
                pages_all += 1;
            },
            Err(e) => warn!("parsing url {} failed: {}", entry.url, e)
        }
    }

    let client = reqwest::Client::builder()
        .timeout(tokio::time::Duration::from_secs(args.http_timeout))
        .connect_timeout(tokio::time::Duration::from_secs(args.http_connect_timeout))
        .build().unwrap();

    let (terminate_tx, _terminate_rx) = tokio::sync::broadcast::channel(2);

    let terminate_tx_for_delay = terminate_tx.clone();
    if let Some(time_limit) = args.time_limit {
        tokio::spawn(async move {
            info!("will exit after {} seconds from now", time_limit);
            tokio::time::sleep(tokio::time::Duration::from_secs(time_limit)).await;
            warn!("time is up, exiting...");
            terminate_tx_for_delay.send(true).unwrap();
        });
    }

    let terminate_tx_for_ctrlc = terminate_tx.clone();
    ctrlc::set_handler(move || {
        warn!("caught SIGINT, exiting...");
        terminate_tx_for_ctrlc.send(true).unwrap();
    })?;

    let (result_tx, mut result_rx) = mpsc::channel(1024);

    info!("starting workers");
    for (_host, pages_with_planidx) in entries_by_host {
        let client = client.clone();
        let result_tx_loc = result_tx.clone();
        let terminate_rx_loc = terminate_tx.subscribe();
        tokio::spawn(async move {
            fetch_pages_single_origin(client, pages_with_planidx,
                result_tx_loc, terminate_rx_loc, args.wait).await
        });
    }
    std::mem::drop(result_tx);

    let mut last_report_time = std::time::Instant::now();

    let mut pages_processed = 0;
    while let Some(fr) = result_rx.recv().await {
        if last_report_time.elapsed().as_secs() >= 60 {
            info!("processed {} pages out of {}", pages_processed, pages_all);
            last_report_time = std::time::Instant::now();
        }
        pages_processed += 1;
        let (planidx, downloaded, res) = fr;
        plan[planidx].retries += 1;
        match res {
            Ok(body) => {
                plan[planidx].status = "ok".to_string();
                let fe = FetchEntry {
                    url: &plan[planidx].url,
                    title: &plan[planidx].title,
                    published: &plan[planidx].published,
                    downloaded: &downloaded,
                    seen: &plan[planidx].seen,
                    feed: &plan[planidx].feed,
                    body: &body,
                };
                data_out_json.write(&fe)?;    
            },
            Err(e) => {
                plan[planidx].status = e;
            }
        }
    }

    data_out_json.flush()?;
    data_outf.sync_all()?;

    info!("writing output plan");
    for entry in plan {
        plan_out.write_all(entry.into_str().as_bytes())?;
    }
    plan_out.flush()?;
    plan_out.sync_all()?;
    info!("done");
    Ok(())
}
