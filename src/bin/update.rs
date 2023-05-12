use std::io::BufRead;
use std::error::Error;
use std::collections::HashMap;
use tokio::sync::mpsc;
use log::*;
use feed_fetcher::{EntryInfo, read_plan, url_to_host};
use clap::Parser;

const VERSION: &str = git_version::git_version!(args=["--tags","--always", "--dirty"]);

/// Update fetch plan from a list of feeds
#[derive(Parser, Debug)]
#[clap(author, version=VERSION, about)]
struct Args {
    /// input plan, can be an empty file
    plan_in: String,

    /// output plan to be written
    plan_out: String,

    /// list of feeds to check
    feeds: String,
}

fn read_feeds(fname: &str) -> Result<
        (HashMap<String, Vec<String>>, usize),
        Box<dyn Error + Sync + Send>> {
    let f = std::fs::File::open(fname)?;
    let br = std::io::BufReader::new(f);
    let mut ht = std::collections::HashMap::new();
    let mut total_feeds = 0;
    for line in br.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() { continue; }
        let host_str = match crate::url_to_host(line) {
            Ok(s) => s,
            Err(e) => {
                warn!("parsing feed URL ({}) failed: {}", line, e);
                continue;
            }
        };
        total_feeds += 1;
        ht.entry(host_str)
            .or_insert_with(Vec::new)
            .push(line.to_string());
    }
    Ok((ht, total_feeds))
}

async fn fetch_feed(client: &reqwest::Client, feed_url: &str) ->
        Result<bytes::Bytes, Box<dyn Error + Sync + Send>> {
    let response = client.get(feed_url).send().await?;
    if response.status() != 200 {
        return Err(format!("got status {}", response.status()).into());
    }
    let body = response.bytes().await?;
    Ok(body)
}

async fn process_feed(body: &mut bytes::Bytes, feed_url: &str) ->
        Result<Vec<EntryInfo>, Box<dyn Error + Sync + Send>> {
    use bytes::Buf;
    let feed = feed_rs::parser::parse_with_uri(body.reader(), Some(feed_url))?;
    Ok(feed.entries.into_iter().filter_map(
        |entry| {
            if entry.links.is_empty() { return None; }
            Some(EntryInfo {
                age: 0,
                status: "new".to_string(),
                retries: 0,
                seen: chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                published: if let Some(date) = entry.published {
                    date.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                } else { "".to_string() },
                url: entry.links[0].href.to_string(),
                title: entry.title.as_ref()
                    .map(|v| v.content.to_string())
                    .unwrap_or("===NONE===".to_string()),
            })
        })
        .collect()
    )
}

async fn fetch_feeds_single_origin(
        client: reqwest::Client, feed_urls: Vec<String>,
        tx: tokio::sync::mpsc::Sender<Vec<EntryInfo>>) ->
            Result<(), Box<dyn Error + Sync + Send>> {
    for feed_url in feed_urls {
        match fetch_feed(&client, &feed_url).await {
            Ok(mut body) => match process_feed(&mut body, &feed_url).await {
                Ok(ei) => tx.send(ei).await?,
                Err(e) => warn!("processing feed failed ({}): {}", &feed_url, e),
            },
            Err(e) => warn!("request failed ({}): {}", &feed_url, e),
        }
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

    let (feeds_by_host, total_feeds) = read_feeds(&args.feeds)?;
    let mut plan_out = std::fs::File::create(&args.plan_out)?;
    let mut plan = read_plan(&args.plan_in)?;

    let mut url_to_planidx = HashMap::<String, usize>::new();
    for (planidx, entry) in plan.iter().enumerate() {
        url_to_planidx.insert(entry.url.to_string(), planidx);
    }

    let (tx, mut rx) = mpsc::channel::<Vec<EntryInfo>>(1024);

    let client = reqwest::Client::builder()
        .timeout(tokio::time::Duration::from_secs(10))
        .connect_timeout(tokio::time::Duration::from_secs(5))
        .build().unwrap();
    
    for (_host, feeds) in feeds_by_host {
        let client = client.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            fetch_feeds_single_origin(client, feeds, tx).await
        });
    }

    for entry in &mut plan {
        entry.age += 1
    }
    let last_previous_planidx = plan.len()-1;
    
    let mut feeds_all = 0;
    let mut feeds_with_entries = 0;
    let mut feeds_with_new_entries = 0;
    let mut entries_all = 0;
    let mut entries_new = 0;
    let mut entries_seen_multiple_times_in_new_plan = 0;

    let mut last_report_time = std::time::Instant::now();

    while let Some(fr) = rx.recv().await {
        if last_report_time.elapsed().as_secs() >= 60 {
            info!("processed {} feeds out of {}", feeds_all, total_feeds);
            last_report_time = std::time::Instant::now();
        }
        feeds_all += 1;
        feeds_with_entries += if fr.is_empty() { 0 } else { 1 };
        let mut feed_has_new_entries = false;
        for entry in fr {
            entries_all += 1;
            if let Some(planidx) = url_to_planidx.get(&entry.url) {
                if *planidx > last_previous_planidx {
                    entries_seen_multiple_times_in_new_plan += 1;
                } else {
                    plan[*planidx].age = 0;
                }
                continue;
            }
            feed_has_new_entries = true;
            entries_new += 1;

            url_to_planidx.insert(entry.url.to_string(), plan.len());
            plan.push(entry);
        }
        feeds_with_new_entries += if feed_has_new_entries { 1 } else { 0 };
    }

    info!("feeds {} total checked, {} with entries, {} with new entries",
          feeds_all, feeds_with_entries, feeds_with_new_entries);
    info!("entries {} seen, {} unseen before, {} seen multiple times in this update",
          entries_all, entries_new, entries_seen_multiple_times_in_new_plan);

    info!("writing output");
    for entry in plan {
        use std::io::Write;
        plan_out.write_all(entry.into_str().as_bytes())?;
    }
    info!("done");
    Ok(())
}
