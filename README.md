# FeedFetcher

Robust web feed (RSS, Atom, JSONFeed) downloader for corpus linguistics.

## Usage

To periodically monitor and download articles from a list of web feeds, follow these steps:

1. Create a suitable directory for the FeedFetcher state, for example, `~/feed_download/`.
2. Create a directory for the downloaded articles, e.g. `~/feed_download/pages/`.
3. Gather a list of RSS/Atom feed URLs and store them in a text file, one URL per line, e.g. `~/feed_download/feeds.txt`.
4. Create an empty feed download plan in the target directory: `touch ~/feed_download/plan.txt`.
5. Periodically run `update` to download all web feeds, check them for changes and update the download plan, and `fetch` to download new articles according to the plan:
  1. `update ~/feed_download/plan.txt ~/feed_download/feeds.txt`
  2. `fetch ~/feed_download/plan.txt ~/feed_download/pages/ --out-prefix-add-timestamp`

The output files in a JSONLines format will be written to the `~/feed_download/pages/` directory. Each record represents a single feed entry (article) and contains the following fields:
 - `url`: URL of the entry.
 - `feed`: URL of the feed, in which the entry was seen.
 - `title`: Title provided by the feed.
 - `published`: Publication time provided by the feed.
 - `seen`: Time of the article appearance in the feed as seen by `update`.
 - `downloaded`: Time of the article download by `fetch`.
 - `body`: Contents of the feed entry.
   
You can use the prebuilt `fetch` and `update` binaries for Linux/x86_64 located in the `bin/` subdirectory of this repository, or build your own according to the instructions in the "Installation" section.

## Installation

FeedFetcher uses the Rust programming language. Follow the instructions at [rustup.rs](https://rustup.rs/) to setup a Rust build environment. Then follow these steps:

1. Check out this git repository: `git checkout <URL>
2. `cd feed_fetcher`
3. Download the dependencies and compile the program: `cargo build --release`

The `target/` directory now contains the binaries.

To enable building Linux static binaries using musl libc, uncomment the `rustflags` and `target` entries in `.cargo/config.toml` and install musl toolchain using `rustup target add x86_64-unknown-linux-musl`. Then, `cargo build` will produce statically linked binaries without any dependencies.
