use async_stream::stream;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use std::convert::Infallible;
use std::io::Write;
use warp::{Filter, sse::Event, http};
use futures_util::{stream::iter, Stream};
//use parking_lot::{RwLock, RwLockReadGuard,MappedRwLockReadGuard};
use std::sync::{Arc, Mutex};
use std::{fmt, str};
use tokio::time::{sleep};
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use warp::{http::Method};
use chrono::{DateTime, TimeZone, NaiveDateTime, Local, Duration};
use rand::{distributions::Uniform, Rng};

pub fn serialize_dt<S>(dt: &DateTime<Local>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
  dt.to_rfc3339().serialize(serializer)
}

fn deserialize_dt<'de, D>(deserializer: D) -> Result<DateTime<Local>, D::Error>
  where D: Deserializer<'de>,
 {
  // Deserialize from a human-readable string like "2015-05-15T17:01:00Z".
  let s = String::deserialize(deserializer)?;
  let dt_offset = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
  Ok(dt_offset.into())
}   
   
#[derive(Debug, Deserialize, Serialize, Clone)]
struct Message {
  #[serde(serialize_with = "serialize_dt")]
  #[serde(deserialize_with = "deserialize_dt")]
  timestamp: DateTime<Local>,
  ticker: Option<String>, 
  price: f64,
  volume: i32,
  direction: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Order {
  i: String,
  d: String,
  p: f64,
  v: i64,
  m: i64,
}

impl Order {
  fn new() -> Self {
    Self {
          i: String::new(),
          d: String::new(),
          p: 0.0,
          v: 0,
          m: 0,
        }
    }
}


impl Message {
  fn new() -> Self {
    Self {
          timestamp: Local::now(),
          ticker: None,
          price: 0.0,
          volume: 0,
          direction: "NA".to_string(),
        }
    }
}

impl fmt::Display for Message {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

#[derive(Clone)]
struct Store {
  messages: Arc<RwLock<Vec<Message>>>
}

use lazy_static::lazy_static;
lazy_static! {
  static ref OFFSET: AtomicUsize = AtomicUsize::new(0);
}

impl Store {
  fn new() -> Self {
    Store {
            messages: Arc::new(RwLock::new(
            vec![Message::new(); 1]
         )),
      }
    }
}

fn json_body() -> impl Filter<Extract = (Message,), Error = warp::Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
} 

fn json_order_body() -> impl Filter<Extract = (Order,), Error = warp::Rejection> + Clone {
  // When accepting a body, we want a JSON body
  // (and to reject huge payloads)...
  warp::body::content_length_limit(1024 * 16).and(warp::body::json())
} 

use warp::reject::Reject;
use std::fs::OpenOptions;

#[derive(Debug)]
struct BlockedWriter;

impl Reject for BlockedWriter {}

lazy_static! {
  static ref ORDER_PIPE: Mutex<File> = { 
    let file_name = "/home/azler/quik/pipes/WindowsPipe/rust_pipe/clientToServer.fifo";
    println!("File path: {:?}", file_name);
    let file = OpenOptions::new()
      .write(true)
      .append(true)
      .open(file_name)
      .unwrap();
      Mutex::new(file)
  };
}

async fn send_order(item: Order)-> Result<impl warp::Reply, warp::Rejection> {
  println!("received order: {:?}", item);
  {
    let mut fpipe = ORDER_PIPE.lock().unwrap();
    let order = serde_json::to_string(&item).unwrap();
    let _ = fpipe.write_all(order.as_bytes());
    fpipe.flush();
  }
  Ok(warp::reply::with_status(
    "send order",
     http::StatusCode::CREATED,
  ))  
}

async fn add_quick_message(
  item: Message,
  store: Store,
  ) -> Result<impl warp::Reply, warp::Rejection> {
  println!("adding quick message: {:?}", item);
  let mut lock = store.messages.try_write();
  if let Err(v) = lock {
    println!("error: {:?}", v);
    return Err(warp::reject::custom(BlockedWriter));
  }
  lock.unwrap().push(item.clone());
  println!("added to store {:?}", item);
  Ok(warp::reply::with_status(
    "added tick to feed",
    http::StatusCode::CREATED,
  ))   
}

use nix::sys::wait::wait;
use nix::unistd::ForkResult::{Child, Parent};
use nix::unistd::{fork, getpid, getppid};
use warp::body::content_length_limit;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio_stream::{self as stream, StreamExt};
use std::fs::File;

#[tokio::main]
async fn run_server(fd: RawFd) -> anyhow::Result<()> {
  pretty_env_logger::init();
  let mut store = Store::new();
  let cors = warp::cors()
    .allow_any_origin()
    .allow_headers(vec![
      "Access-Control-Allow-Headers", 
      "Access-Control-Request-Method", 
      "Access-Control-Request-Headers", 
      "Origin", 
      "Accept", 
      "X-Requested-With", 
      "Content-Type"
      ])
      .allow_methods(&[Method::GET, Method::POST, Method::DELETE]);
  let store_filter = warp::any().map(move || store.clone());
  let fd_filter = warp::any().map(move || fd);
  let messages = warp::post()
    .and(warp::path("messages"))
    .and(json_body())
    .and(store_filter.clone())
    .and_then(add_quick_message).with(cors.clone());

  let send_order = warp::post()
    .and(warp::path("orders"))
    .and(json_order_body())
    .and_then(send_order).with(cors.clone()); 

  let sse_ticks = warp::get()
    .and(warp::path("ticks"))
    .and(fd_filter)
    .map(move |val: RawFd| {
      warp::sse::reply(warp::sse::keep_alive().stream(sse_events(val)))
    }).with(cors);
      
  let routes = sse_ticks.or(messages).or(send_order);
  warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
  Ok(())
}

use std::os::unix::net::{UnixDatagram};
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use sendfd::{SendWithFd, RecvWithFd};
use nix::unistd::{close, dup2, lseek, read, write, Whence};
use std::str::from_utf8;
use std::mem::transmute;
use core::slice::from_raw_parts;
use std::ffi::CStr;
use std::thread;
use futures::executor::block_on;

#[derive(Debug)]
#[repr(C)]
struct QueueHeader {
  readIdx: u64,
  writeIdx: u64,
  availableIdx: u64,
}

const SLOT_SIZE: usize = 512;

fn receiveMessages(buf_ptr: &mut [u8], buf_size: usize)-> Vec<Result<Event, Infallible>> {
  let mut offset = 0;
  let mut records: Vec<Result<Event, Infallible>> = vec![];
  loop {
    unsafe { 
      //println!("buff read: {:?}", &buf_ptr[offset..offset + 128]);
      if buf_ptr.len() < offset + SLOT_SIZE {
        println!("insufficient buffer size: {:?}, to read {:?} bytes", buf_ptr.len(), buf_size);
        break;
      }
      let message: String = CStr::from_bytes_until_nul(&buf_ptr[offset..offset + SLOT_SIZE]).unwrap().to_str().unwrap().into();
      records.push(Ok(Event::default().event("message").data(message)));
    };
    offset += SLOT_SIZE;
    if offset >= buf_size {
      break;
    }
  }
  records
}

fn sse_events(fd: RawFd) -> impl Stream<Item = Result<Event, Infallible>> {
  stream! {
  let mut readIdx: u64 = 0;
  let mut prevWriteIdx: u64 = 0;
  let mut queueHeader: [u8; 24] = [0u8; 24];
  let mut nbytes: usize;
  loop {
      lseek(fd, 0, Whence::SeekSet);
      nbytes = read(fd, &mut queueHeader[..]).unwrap();
      let input = unsafe { transmute::<[u8; 24], QueueHeader>(queueHeader) };
      //println!("header transmuted received: {:?}, nbytes {:?}, prevWriteIdx: {:?}", input, nbytes, prevWriteIdx);
      let writeIdx = input.availableIdx;
      if writeIdx == prevWriteIdx  {
        continue;
      }
      let mut dim: usize = (((writeIdx - prevWriteIdx) % 2001) << 9) as usize;
      if  writeIdx - prevWriteIdx > 1000 {
        println!("header transmuted received: {:?}, nbytes {:?}, prevWriteIdx: {:?}", input, nbytes, prevWriteIdx);
      }
      println!("prev write idx: {:?}, {:?}, {:?}", prevWriteIdx, dim, writeIdx);
      let mut messageBody = vec![0u8; dim];
      let offset = (prevWriteIdx % 2000) << 9;
      if offset > 0 {
        lseek(fd, offset as i64, Whence::SeekCur);
      }
      prevWriteIdx = writeIdx;
      nbytes = read(fd, &mut messageBody[..]).unwrap();
      readIdx = writeIdx;
      for item in receiveMessages(&mut messageBody[..], nbytes) {
        println!("yield: {:?} readIdx: {}", item, readIdx);
        yield item;
      }
      let ptr = unsafe { transmute::<&u64, * const u8>(&mut readIdx) };
      let readIdxSlice: &[u8] = unsafe { from_raw_parts::<u8>(ptr, 8) };
      //println!("read idx slice: {:?}", readIdxSlice);
      lseek(fd, 0, Whence::SeekSet);
      let wbytes = write(fd, readIdxSlice);
    }
  }
}

fn main() -> anyhow::Result<()> {
  let (l, r) = UnixDatagram::pair().expect("create UnixDatagram pair");
  let pid = unsafe { fork() };
  match pid.expect("Fork Failed: Unable to create child process!") {
    Child => { 
      close(l.into_raw_fd()).unwrap();
      dup2(r.as_raw_fd(), 1);
      close(r.into_raw_fd()).unwrap();
      let err = exec::execvp("wine", &["wine", "windows/shared.exe"]);
    Ok(())
    },
    Parent { child } => {
      let mut recv_bytes = [0; 128];
      let mut recv_fds = [0, 0, 0, 0, 0, 0, 0];
      close(r.into_raw_fd()).unwrap();
      let mut fd = 0;
      loop {
      l.recv_with_fd(&mut recv_bytes, &mut recv_fds).expect("recv should be successful");
      println!(
        "Wine Reader parent process with pid: {} and child pid:{}, received fds: {:?}, bytes: {:?}",
        getpid(),
        child, 
        recv_fds,
        from_utf8(&recv_bytes[..])
      );
      fd = recv_fds[0];
      if fd > 0 {
        break;
      }
      }
      run_server(fd) 
    }
  } 
}
