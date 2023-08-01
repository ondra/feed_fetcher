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

    /// HTTP client timeout
    #[arg(long,default_value_t=120)]
    http_timeout: u64,

    /// HTTP client timeout for the connection phase only
    #[arg(long,default_value_t=60)]
    http_connect_timeout: u64,

    /// delay between successive HTTP request to the same server
    #[arg(long,default_value_t=3)]
    wait: u64,

    /// max active concurrent connections (idle connections can exceed this)
    #[arg(long,default_value_t=500)]
    max_concurrent_connections: usize,
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
        tx: tokio::sync::mpsc::Sender<(String, Result<Vec<EntryInfo>, Box<dyn Error + Sync + Send>>)>,
        sem: std::sync::Arc<tokio::sync::Semaphore>,
        mut terminate_rx: tokio::sync::broadcast::Receiver<bool>, wait: u64) ->
            Result<(), Box<dyn Error + Sync + Send>> {
    let mut first = true;
    for feed_url in feed_urls {
        match terminate_rx.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {},
            _ => break,
        }
        if first { first = false; } else {
            tokio::time::sleep(tokio::time::Duration::from_secs(wait)).await
        }
        let fetch_res = {
            let _permit = sem.acquire().await.unwrap();
            fetch_feed(&client, &feed_url).await
        };
        let proc_res = match fetch_res {
            Ok(mut body) => match process_feed(&mut body, &feed_url).await {
                Ok(ei) => Ok(ei),
                Err(e) => Err(format!("processing feed failed ({}): {}", &feed_url, e).into()),
            },
            Err(e) => Err(format!("request failed ({}): {}", &feed_url, e).into()),
        };
        tx.send((feed_url, proc_res)).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")).init();
    info!("update ver {} starting", &VERSION);

    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    info!("reading feeds from {}", &args.feeds);
    let (mut feeds_by_host, total_feeds) = read_feeds(&args.feeds)?;

    let planfile_bkp = args.planfile.to_string() + "." + &ts + ".update.bkp";

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

    let planfile_tmp = args.planfile.to_string() + "." + &ts + ".update.tmp";
    info!("opening output plan at {}", &planfile_tmp);
    let mut plan_out = std::fs::File::create(&planfile_tmp)?;

    info!("reading plan from {}", &planfile_bkp);
    let mut plan = read_plan(&planfile_bkp)?;
    info!("there are {} plan elements in total", plan.len());

    let mut rng = rand::thread_rng();
    for (_host, feeds) in feeds_by_host.iter_mut() {
        use rand::seq::SliceRandom;
        feeds.shuffle(&mut rng);
    }

    let mut url_to_planidx = HashMap::<String, usize>::new();
    for (planidx, entry) in plan.iter().enumerate() {
        url_to_planidx.insert(entry.url.to_string(), planidx);
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

    let (result_tx, mut result_rx) = mpsc::channel::<(String,
            Result<Vec<EntryInfo>, Box<dyn Error + Sync + Send>>)>(1024);

    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(
            args.max_concurrent_connections));

    for (_host, feeds) in feeds_by_host {
        let client = client.clone();
        let sem_loc = sem.clone();
        let result_tx_loc = result_tx.clone();
        let terminate_rx_loc = terminate_tx.subscribe();
        tokio::spawn(async move {
            fetch_feeds_single_origin(client, feeds,
                result_tx_loc, sem_loc, terminate_rx_loc, args.wait).await
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

    while let Some(feed_data) = result_rx.recv().await {
        if last_report_time.elapsed().as_secs() >= 60 {
            info!("processed {} feeds out of {}, {} with entries, {} with new entries",
                  feeds_all, total_feeds,
                  feeds_with_entries, feeds_with_new_entries);
            last_report_time = std::time::Instant::now();
        }
        feeds_all += 1;
        let (_feed_url, feed_res) = feed_data;
        match feed_res {
            Ok(fr) => {
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
            },
            Err(e) => {
                warn!("{}", &e);
            }
        }
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
        if entry.age > 1 && (entry.status == "ok" || entry.retries >= 5) {
            // do not store stale entries
        } else {
            plan_out.write_all(entry.into_str().as_bytes())?;
        }
    }
    plan_out.flush()?;
    plan_out.sync_all()?;
    info!("renaming temporary plan");
    std::fs::rename(&planfile_tmp, &args.planfile)?;
    info!("done");
    Ok(())
}
