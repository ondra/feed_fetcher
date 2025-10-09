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
    body_base64: &'a str,
    content_type: &'a Vec<String>,
}

const VERSION: &str = git_version::git_version!(args=["--tags","--always", "--dirty"]);

/// Fetch pages according to the plan
#[derive(Parser, Debug)]
#[clap(author, version=VERSION, about)]
struct Args {
    /// processing plan
    planfile: String,

    /// jsonlines fetched data output file
    out_prefix: String,

    /// time in seconds, after which the program will terminate
    #[arg(long)]
    time_limit: Option<u64>,

    /// compress the output with zstd
    #[arg(long,default_value_t=false)]
    compress: bool,

    /// do not truncate the output file, append to it
    #[arg(long,default_value_t=false)]
    append: bool,

    /// HTTP client timeout in seconds
    #[arg(long,default_value_t=120)]
    http_timeout: u64,

    /// HTTP client timeout in seconds, for the connection phase only
    #[arg(long,default_value_t=60)]
    http_connect_timeout: u64,

    /// delay in seconds between successive HTTP request to the same server
    #[arg(long,default_value_t=5)]
    wait: u64,

    /// append the current timestamp to the output filename
    #[arg(long,default_value_t=false)]
    out_prefix_add_timestamp: bool,

    /// max active concurrent connections (idle connections can exceed this)
    #[arg(long,default_value_t=500)]
    max_concurrent_connections: usize,
}

async fn fetch_page(client: &reqwest::Client, page_url: &str) ->
        Result<(bytes::Bytes, Vec<String>), String> {
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
    let content_type = response.headers()
        .get_all(reqwest::header::CONTENT_TYPE)
        .iter()
        .map(|x| x.to_str().unwrap_or_default().to_string())
        .collect::<Vec<String>>();
    let body = match response.bytes().await {
        Ok(body) => {
            (body, content_type)
        },
        Err(e) => {
            warn!("response for {} failed: {}", page_url, e);
            return Err("res".to_string());
        },
    };
    Ok(body)
}

async fn fetch_pages_single_origin(
        client: reqwest::Client, url_with_id: Vec<(usize, String)>,
        tx: tokio::sync::mpsc::Sender<(usize, String,
            Result<(bytes::Bytes, Vec<String>), String>)>,
        sem: std::sync::Arc<tokio::sync::Semaphore>,
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
        tx.send((planidx,
            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            {
                let _permit = sem.acquire().await.unwrap();
                fetch_page(&client, &page_url).await
            }
        )).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")).init();
    info!("fetch ver {} starting", &VERSION);

    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let data_out_fname = args.out_prefix;
    let data_out_fname = if args.out_prefix_add_timestamp {
        data_out_fname + &ts
    } else { data_out_fname } + ".jsonl";
    let data_out_fname = if args.compress {
        data_out_fname + ".zstd"
    } else { data_out_fname };
    info!("opening data output at {}", &data_out_fname);
    let data_outf = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(args.append)
        .truncate(!args.append)
        .open(data_out_fname)?;
    let data_wr: Box<dyn std::io::Write> = if args.compress {
        Box::new(zstd::Encoder::new(&data_outf, 0)?.auto_finish())
    } else {
        Box::new(&data_outf)
    };
    let mut data_out_json = serde_jsonlines::JsonLinesWriter::new(data_wr);

    let planfile_bkp = args.planfile.to_string() + "." + &ts + ".fetch.bkp";
    info!("moving plan to {}", &planfile_bkp);
    if let Err(e) = std::fs::rename(&args.planfile, &planfile_bkp) {
        match e.kind() {
            std::io::ErrorKind::NotFound => {
                warn!("plan not found, exiting");
                return Ok(());
            },
            _ => {
                error!("plan move failed: {}", e);
                return Err(e.into());
            },
        }
    };

    let planfile_tmp = args.planfile.to_string() + "." + &ts + ".fetch.tmp";
    info!("opening output plan at {}", &planfile_tmp);
    let mut plan_out = std::fs::File::create(&planfile_tmp)?;

    info!("reading plan from {}", &planfile_bkp);
    let mut plan = read_plan(&planfile_bkp)?;
    info!("there are {} plan elements in total", plan.len());

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

    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(
            args.max_concurrent_connections));

    info!("{} pages scheduled for download", pages_all);
    info!("starting workers");
    for (_host, pages_with_planidx) in entries_by_host {
        let client = client.clone();
        let result_tx_loc = result_tx.clone();
        let sem_loc = sem.clone();
        let terminate_rx_loc = terminate_tx.subscribe();
        tokio::spawn(async move {
            fetch_pages_single_origin(client, pages_with_planidx,
                result_tx_loc, sem_loc, terminate_rx_loc, args.wait).await
        });
    }
    std::mem::drop(result_tx);

    let mut last_report_time = std::time::Instant::now();

    let mut pages_procd = 0;
    let mut pages_err = 0;
    let mut pages_ok = 0;
    while let Some(fr) = result_rx.recv().await {
        if last_report_time.elapsed().as_secs() >= 60 {
            info!("processed {} pages out of {}", pages_procd, pages_all);
            last_report_time = std::time::Instant::now();
        }
        pages_procd += 1;
        let (planidx, downloaded, res) = fr;
        plan[planidx].retries += 1;
        match res {
            Ok((body, content_type)) => {
                plan[planidx].status = "ok".to_string();
                use base64::Engine as _;
                let body_encoded = base64::engine::general_purpose::STANDARD
                    .encode(&body);
                let fe = FetchEntry {
                    url: &plan[planidx].url,
                    title: &plan[planidx].title,
                    published: &plan[planidx].published,
                    downloaded: &downloaded,
                    seen: &plan[planidx].seen,
                    feed: &plan[planidx].feed,
                    body_base64: &body_encoded,
                    content_type: &content_type,
                };
                data_out_json.write(&fe)?;    
                pages_ok += 1;
            },
            Err(e) => {
                plan[planidx].status = e;
                pages_err += 1;
            }
        }
    }

    data_out_json.flush()?;
    std::mem::drop(data_out_json);
    data_outf.sync_all()?;

    info!("processed {} total pages, {} ok, {} err",
          pages_procd, pages_ok, pages_err);
    info!("writing output plan");
    for entry in plan {
        plan_out.write_all(entry.into_str().as_bytes())?;
    }
    plan_out.flush()?;
    plan_out.sync_all()?;
    info!("renaming temporary plan");
    std::fs::rename(&planfile_tmp, &args.planfile)?;
    info!("done");
    Ok(())
}
