use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
  },
  thread::JoinHandle,
  time::{Duration, Instant},
};
use tracing::{debug, error, instrument};

use crossbeam_channel::{select, Receiver, RecvError, Sender};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[derive(Debug)]
struct Config {
  /// The number of requests that can be accepted every second.
  requests_per_second: usize,
}

#[derive(Debug)]
struct Bucket {
  config: Config,
  /// How many requests we can accept at this time.
  tokens: AtomicUsize,
  /// Sends are actually never made in this channel.
  /// It is used only for the worker thread to know when the bucket
  /// has been dropped and exit.
  close_channel_sender: Sender<()>,
}

impl Bucket {
  #[instrument(skip_all, fields(config = ?config))]
  pub fn new(config: Config) -> Arc<Self> {
    let (sender, receiver) = crossbeam_channel::unbounded::<()>();

    let tokens = AtomicUsize::new(1);

    let bucket = Arc::new(Self {
      config,
      tokens,
      close_channel_sender: sender,
    });

    let bucket_clone = Arc::downgrade(&bucket);
    std::thread::spawn(move || Bucket::add_tokens_to_bucket_on_interval(bucket_clone, receiver));

    bucket
  }

  /// Returns true if there's enough tokens in the bucket.
  pub fn acquire(&self) -> bool {
    self
      .tokens
      .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |tokens| {
        Some(if tokens > 0 { tokens - 1 } else { tokens })
      })
      .map(|tokens_in_the_bucket| tokens_in_the_bucket > 0)
      .unwrap_or(false)
  }

  fn add_tokens_to_bucket_on_interval(bucket: Weak<Bucket>, receiver: Receiver<()>) {
    let interval = {
      match bucket.upgrade() {
        None => {
          error!(
            "unable to define interval to add tokens to bucket because bucket has been dropped"
          );
          return;
        }
        Some(bucket) => Duration::from_secs_f64(1.0 / (bucket.config.requests_per_second as f64)),
      }
    };

    debug!(?interval, "will add tokens to bucket at interval");

    let ticker = crossbeam_channel::tick(interval);

    loop {
      select! {
        recv(ticker) -> message => match message {
          Err(e) => {
            error!(?e);
            return;
          }
          Ok(_) => match bucket.upgrade() {
            None => {
              debug!("cannot upgrade Weak ref to Arc, exiting");
              return;
            }
            Some(bucket) => {
              let _ = bucket
                .tokens
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |tokens| {
                  Some(std::cmp::min(tokens + 1, bucket.config.requests_per_second))
                });

            }
          },
        },
        recv(receiver) -> message => {
          // An error is returned when we try to received from a channel that has been closed
          // and this channel will only be closed when the bucket is dropped.
          if message == Err(RecvError) {
            debug!("bucket has been dropped, won't add try to add tokens to the bucket anymore");
            return;
          }
        }
      }
    }
  }
}

fn main() {
  let (non_blocking_writer, _guard) = tracing_appender::non_blocking(std::io::stdout());

  let app_name = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION")).to_string();

  let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

  let subscriber = Registry::default()
    .with(JsonStorageLayer)
    .with(bunyan_formatting_layer);

  tracing::subscriber::set_global_default(subscriber).unwrap();

  let bucket = Bucket::new(Config {
    requests_per_second: 50,
  });

  let requests = Arc::new(AtomicUsize::new(0));
  let start = Instant::now();

  let _handles: Vec<JoinHandle<()>> = (0..10)
    .map(|_| {
      let bucket = Arc::clone(&bucket);
      let requests = Arc::clone(&requests);

      std::thread::spawn(move || loop {
        while !bucket.acquire() {}

        let _ = requests.fetch_add(1, Ordering::SeqCst);
      })
    })
    .collect();

  std::thread::sleep(Duration::from_secs(5));

  println!(
    "{:?} requests have been made in {:?} seconds",
    &requests,
    start.elapsed()
  );
}
