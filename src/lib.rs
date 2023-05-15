use std::io::BufRead;
use std::error::Error;
use log::*;

pub fn url_to_host(url: &str) -> Result<String, Box<dyn Error>> {
    let parsed_url = url::Url::parse(url)?;
    match parsed_url.host_str() {
        Some(s) => Ok(s.to_string()),
        None => Err("URL has no hostname".into()),
    }
}

#[derive(Debug)]
pub struct EntryInfo {
    pub age: u32,
    pub status: String,
    pub retries: u32,
    pub seen: String,
    pub published: String,
    pub feed: String,
    pub url: String,
    pub title: String,
}

impl EntryInfo {
    pub fn from_str(s: &str) -> Result<EntryInfo, Box<dyn Error>> {
        let parts = s.split('\t').collect::<Vec<&str>>();
        if parts.len() != 7 {
            Err("wrong number of elements per line!".into())
        } else {
            Ok(EntryInfo {
                age: str::parse::<u32>(parts[0])?,
                status: parts[1].to_string(),
                retries: str::parse::<u32>(parts[2])?,
                seen: parts[3].to_string(),
                published: parts[4].to_string(),
                feed: parts[5].to_string(),
                url: parts[6].to_string(),
                title: parts[7].to_string(),
            })
        }
    }
    pub fn into_str(self) -> String {
        let sage = format!("{}", self.age);
        let sretries = format!("{}", self.retries);
        [sage, self.status, sretries, self.seen, self.published, self.feed, self.url, self.title]
            .map(|s| s.replace('\t', ""))
            .join("\t") + "\n"
    }
}

pub fn read_plan(fname: &str) -> Result<
        Vec<EntryInfo>, Box<dyn Error + Sync + Send>> {
    let f = std::fs::File::open(fname)?;
    let br = std::io::BufReader::new(f);
    let mut plan = Vec::new();
    for line in br.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() { continue; }
        let ei = match EntryInfo::from_str(line) {
            Ok(ei) => ei, 
            Err(e) => {
                warn!("parsing plan entry failed: {}\n{}", e, line);
                continue;
            }
        };
        plan.push(ei);
    }
    Ok(plan)
}

