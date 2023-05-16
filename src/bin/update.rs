use std::io::BufRead;
use std::error::Error;
use std::collections::HashMap;
use tokio::sync::mpsc;
use log::*;
use feed_fetcher::{EntryInfo, read_plan, url_to_host};
use clap::Parser;
use std::io::Write;

const VERSION: &str = git_version::git_version!(args=["--tags","--always", "--dirty"]);

/// Update fetch plan from a list of feeds
#[derive(Parser, Debug)]
#[clap(author, version=VERSION, about)]
struct Args {
    /// processing plan
    planfile: String,

    /// list of feeds to check
    feeds: String,

    /// time in seconds, after which I will give up
    #[arg(long)]
    time_limit: Option<u64>,
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
                feed: feed_url.to_string(),
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
        tx: tokio::sync::mpsc::Sender<Vec<EntryInfo>>,
        mut terminate_rx: tokio::sync::broadcast::Receiver<bool>) ->
            Result<(), Box<dyn Error + Sync + Send>> {
    let mut first = true;
    for feed_url in feed_urls {
        match terminate_rx.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {},
            _ => break,
        }
        if first { first = false; } else {
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await
        }
        match fetch_feed(&client, &feed_url).await {
            Ok(mut body) => match process_feed(&mut body, &feed_url).await {
                Ok(ei) => tx.send(ei).await?,
                Err(e) => warn!("processing feed failed ({}): {}", &feed_url, e),
            },
            Err(e) => warn!("request failed ({}): {}", &feed_url, e),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")).init();

    let mut rng = rand::thread_rng();
    let (mut feeds_by_host, total_feeds) = read_feeds(&args.feeds)?;
    for (_host, feeds) in feeds_by_host.iter_mut() {
        use rand::seq::SliceRandom;
        feeds.shuffle(&mut rng);
    }

    info!("reading plan from {}", &args.planfile);
    let mut plan = read_plan(&args.planfile)?;
    info!("there are {} plan elements in total", plan.len());

    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let planfile_bkp = args.planfile.to_string() + "." + &ts + ".update.bkp";
    info!("moving plan to {}", &planfile_bkp);
    std::fs::rename(&args.planfile, planfile_bkp)?;

    let mut plan_out = std::fs::File::create(&args.planfile)?;

    let mut url_to_planidx = HashMap::<String, usize>::new();
    for (planidx, entry) in plan.iter().enumerate() {
        url_to_planidx.insert(entry.url.to_string(), planidx);
    }

    let client = reqwest::Client::builder()
        .timeout(tokio::time::Duration::from_secs(10))
        .connect_timeout(tokio::time::Duration::from_secs(5))
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

    let (result_tx, mut result_rx) = mpsc::channel::<Vec<EntryInfo>>(1024);

    for (_host, feeds) in feeds_by_host {
        let client = client.clone();
        let result_tx_loc = result_tx.clone();
        let terminate_rx_loc = terminate_tx.subscribe();
        tokio::spawn(async move {
            fetch_feeds_single_origin(client, feeds,
                result_tx_loc, terminate_rx_loc).await
        });
    }
    std::mem::drop(result_tx);

    let mut seen_previous_planidxs = std::collections::HashSet::<usize>::new();
    let previous_planlen = plan.len();
    
    let mut feeds_all = 0;
    let mut feeds_with_entries = 0;
    let mut feeds_with_new_entries = 0;
    let mut entries_all = 0;
    let mut entries_new = 0;
    let mut entries_seen_multiple_times_in_new_plan = 0;

    let mut last_report_time = std::time::Instant::now();

    while let Some(fr) = result_rx.recv().await {
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
                if *planidx >= previous_planlen {
                    entries_seen_multiple_times_in_new_plan += 1;
                } else {
                    seen_previous_planidxs.insert(*planidx);
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

    info!("feeds: {} total checked, {} with entries, {} with new entries",
          feeds_all, feeds_with_entries, feeds_with_new_entries);
info!("entries: {} seen, {} unseen before, {} seen multiple times in this update {} not visible anymore",
          entries_all, entries_new, entries_seen_multiple_times_in_new_plan, previous_planlen - seen_previous_planidxs.len());

    info!("updating seen entries");
    for planidx in 0..previous_planlen {
        if seen_previous_planidxs.contains(&planidx) {
            plan[planidx].age = 0;
        } else {
            plan[planidx].age += 1;
        }
    }

    info!("writing output");
    for entry in plan {
        plan_out.write_all(entry.into_str().as_bytes())?;
    }
    plan_out.flush()?;
    plan_out.sync_all()?;
    info!("done");
    Ok(())
}
