use std::error::Error;
use std::collections::HashMap;
use tokio::sync::mpsc;
use log::*;
use feed_fetcher::{read_plan, url_to_host};
use clap::Parser;

use serde_jsonlines;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct FetchEntry<'a> {
    url: &'a str,
    title: &'a str,
    published: &'a str,
    seen: &'a str,
    fetched: &'a str,
    body: &'a str,
}

const VERSION: &str = git_version::git_version!(args=["--tags","--always", "--dirty"]);

/// Update fetch plan from a list of feeds
#[derive(Parser, Debug)]
#[clap(author, version=VERSION, about)]
struct Args {
    /// input plan, can be an empty file
    plan_in: String,

    /// output plan file to be written
    plan_out: String,

    /// jsonlines fetched data output file
    data_out: String,
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
        tx: tokio::sync::mpsc::Sender<(usize, String, Result<String, String>)>) ->
            Result<(), Box<dyn Error + Sync + Send>> {
    for (planidx, page_url) in url_with_id {
        let res = fetch_page(&client, &page_url).await;
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        tx.send((planidx, ts, res)).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = Args::parse();
    simplelog::TermLogger::init(
        simplelog::LevelFilter::Trace,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Auto,
    )?;

    let mut plan_out = std::fs::File::create(&args.plan_out)?;
    let data_out = std::fs::File::create(&args.data_out)?;
    let mut data_out_json = serde_jsonlines::JsonLinesWriter::new(data_out);
    let mut plan = read_plan(&args.plan_in)?;
    info!("there are {} plan elements in total", plan.len());

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

    /*
    let mut url_to_planidx = HashMap::<String, usize>::new();
    for (planidx, entry) in plan.iter().enumerate() {
        url_to_planidx.insert(entry.url.to_string(), planidx);
    }*/

    let (tx, mut rx) = mpsc::channel(1024);

    let client = reqwest::Client::builder()
        .timeout(tokio::time::Duration::from_secs(10))
        .connect_timeout(tokio::time::Duration::from_secs(5))
        .build().unwrap();
    
    for (_host, pages_with_planidx) in entries_by_host {
        let client = client.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            fetch_pages_single_origin(client, pages_with_planidx, tx).await
        });
    }

    let mut last_report_time = std::time::Instant::now();

    let mut pages_processed = 0;
    while let Some(fr) = rx.recv().await {
        if last_report_time.elapsed().as_secs() >= 60 {
            info!("processed {} pages out of {}", pages_processed, pages_all);
            last_report_time = std::time::Instant::now();
        }
        pages_processed += 1;
        let (planidx, fetched, res) = fr;
        plan[planidx].retries += 1;
        match res {
            Ok(body) => {
                plan[planidx].status = "ok".to_string();
                let fe = FetchEntry {
                    url: &plan[planidx].url,
                    title: &plan[planidx].title,
                    published: &plan[planidx].published,
                    fetched: &fetched,
                    seen: &plan[planidx].seen,
                    body: &body,
                };
                data_out_json.write(&fe)?;    
            },
            Err(e) => {
                plan[planidx].status = e;
            }
        }
    }

    info!("writing output plan");
    for entry in plan {
        use std::io::Write;
        plan_out.write_all(entry.into_str().as_bytes())?;
    }
    info!("done");
    Ok(())
}
