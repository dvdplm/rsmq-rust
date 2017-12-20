extern crate radix;
extern crate r2d2;
extern crate r2d2_redis;
extern crate rand;
extern crate redis;

use std::default::Default;
use std::error::Error;
use std::ops::Deref;
use redis::{from_redis_value, RedisError, RedisResult, Value};
use r2d2_redis::RedisConnectionManager;
use r2d2::Pool;

const PONG: &str = "PONG";

#[derive(Clone, Debug)]
pub struct Queue {
  pub qname:      String,
  pub vt:         u64,
  pub delay:      u64,
  pub maxsize:    i64,
  pub totalrecv:  u64,
  pub totalsent:  u64,
  pub created:    u64,
  pub modified:   u64,
  pub msgs:       u64, // current message count
  pub hiddenmsgs: u64, // hidden, aka "in-flight" messages + delayed messages
}

impl Queue {
  pub fn new(qname: &str, vt: Option<u64>, delay: Option<u64>, maxsize: Option<i64>) -> Queue {
    let mut q = Queue {
      ..Default::default()
    };
    q.qname = qname.into();
    q.vt = vt.unwrap_or(30);
    q.delay = delay.unwrap_or(0);
    q.maxsize = maxsize.unwrap_or(65536);
    q
  }
}

impl Default for Queue {
  fn default() -> Queue {
    Queue {
      qname:      "".into(),
      vt:         30,
      delay:      0,
      maxsize:    65536,
      totalrecv:  0,
      totalsent:  0,
      created:    0,
      modified:   0,
      msgs:       0,
      hiddenmsgs: 0,
    }
  }
}

#[derive(Clone, Debug)]
pub struct Message {
  pub id:      String,
  pub message: String,
  pub rc:      u64, // Receive count
  pub fr:      u64, // First receive time
  pub sent:    u64,
}

impl Message {
  pub fn new() -> Message {
    Message {
      id:      "".into(),
      message: "".into(),
      sent:    0,
      fr:      0,
      rc:      0,
    }
  }
}
impl redis::FromRedisValue for Message {
  fn from_redis_value(v: &Value) -> RedisResult<Message> {
    match *v {
      Value::Bulk(ref items) => {
        if items.len() == 0 {
          // TODO: figure out error handling and get rid of this awefulness
          let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "No messages to receive");
          let redis_err = RedisError::from(custom_error);
          return Err(redis_err);
        }
        let mut m = Message::new();
        m.id = from_redis_value(&items[0])?;
        m.message = from_redis_value(&items[1])?;
        m.rc = from_redis_value(&items[2])?;
        m.fr = from_redis_value(&items[3])?;
        // TODO: figure out how to wrap a std::num::ParseIntError in a RedisError so we can use `?` here
        m.sent = u64::from_str_radix(&m.id[0..10], 36).expect("could not convert first 10 chars from redis response to timestamp");
        Ok(m)
      }
      _ => {
        // println!("Oh boy, what even are you? {:?}", v);
        // TODO: figure out how to return something sensible here
        let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Redis did not return a bulk");
        let redis_err = RedisError::from(custom_error);
        return Err(redis_err);
      }
    }
  }
}

pub type RsmqResult<T> = Result<T, Box<Error>>;

// #[derive(Debug)]
pub struct Rsmq {
  pub pool: Pool<RedisConnectionManager>,
  name_space: String,
}

impl std::fmt::Debug for Rsmq {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "redis namespace: {}, {:?}", self.name_space, self.pool) }
}

impl Rsmq {
  pub fn new<T: redis::IntoConnectionInfo>(params: T, name_space: &str) -> Result<Rsmq, Box<Error>> {
    let manager = RedisConnectionManager::new(params)?;
    let pool = r2d2::Pool::new(manager)?;
    let con = pool.get()?;
    let pong: String = redis::cmd("PING").query(con.deref())?; // TODO: it looks like the r2d2 setup code does this already so perhaps we can skip it
    assert_eq!(pong, PONG);
    let ns = if name_space != "" {name_space.into()} else {"rsmq".into()};
    Ok(Rsmq { pool: pool, name_space: ns})
  }

  pub fn create_queue(&self, opts: Queue) -> RsmqResult<u8> {
    let con = self.pool.get()?;
    let qky = self.queue_hash_key(&opts.qname);
    let (ts, _): (u32, u32) = redis::cmd("TIME").query(con.deref())?;
    let (res,): (u8,) = redis::pipe()
      .atomic()
      .cmd("HSETNX").arg(&qky).arg("vt")        .arg(opts.vt) .ignore()
      .cmd("HSETNX").arg(&qky).arg("delay")     .arg(opts.delay) .ignore()
      .cmd("HSETNX").arg(&qky).arg("maxsize")   .arg(opts.maxsize) .ignore()
      .cmd("HSETNX").arg(&qky).arg("totalrecv") .arg(0) .ignore()
      .cmd("HSETNX").arg(&qky).arg("totalsent") .arg(0) .ignore()
      .cmd("HSETNX").arg(&qky).arg("created")   .arg(ts) .ignore()
      .cmd("HSETNX").arg(&qky).arg("modified")  .arg(ts) .ignore()
      .cmd("SADD")  .arg(format!("{}:QUEUES", self.name_space)).arg(opts.qname)
      .query(con.deref())?;
    Ok(res)
  }

  pub fn delete_queue(&self, qname: &str) -> RsmqResult<Value> {
    let con = self.pool.get()?;
    let key = self.message_zset_key(qname);
    redis::pipe()
            .atomic()
            .cmd("DEL").arg(format!("{}:Q", &key)).ignore() // The queue hash
            .cmd("DEL").arg(&key).ignore() // The messages zset
            .cmd("SREM").arg(format!("{}:QUEUES", self.name_space)).arg(qname).ignore()
            .query(con.deref()).map_err(|e| e.into())
  }

  pub fn list_queues(&self) -> RsmqResult<Vec<String>> {
    let con = self.pool.get()?;
    let key = format!("{}:QUEUES", self.name_space);
    redis::cmd("SMEMBERS")
      .arg(key)
      .query(con.deref())
      .map_err(|e| e.into())
  }

  fn get_queue(&self, qname: &str, set_uid: bool) -> RsmqResult<(Queue, u64, Option<String>)> {
    let con = self.pool.get()?;
    let qkey = self.queue_hash_key(qname);
    let ((vt, delay, maxsize), (secs, micros)): ((u64, u64, i64), (u64, u64)) = redis::pipe()
      .atomic()
      .cmd("HMGET").arg(qkey).arg("vt").arg("delay").arg("maxsize")
      .cmd("TIME")
      .query(con.deref())?;

    let ts_micros = secs * 1_000_000 + micros;
    let ts = ts_micros / 1_000; // Epoch time in milliseconds
    let q = Queue {
      qname: qname.into(),
      vt: vt,
      delay: delay,
      maxsize: maxsize,
      ..Default::default()
    };
    // This is a bit crazy. The JS version calls getQueue with the `set_uid` set to `true` only from `sendMessage`
    // where it is used to write the timestamp+random stuff that constituates the (sort)key. This is just a port of
    // that behavior. I don't understand why it is baked in with the queue attrib fetch.
    let uid = if set_uid {
      let ts_rad36 = radix::RadixNum::from(ts_micros).with_radix(36).unwrap().as_str().to_lowercase().to_string();
      Some(ts_rad36 + &make_id_22())
    } else {
      None
    };
    Ok((q, ts, uid))
  }

  pub fn change_message_visibility(&self, qname: &str, msgid: &str, hidefor: u64) -> RsmqResult<u64> {
    const LUA: &'static str = r#"
            local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])
			if not msg then
				return 0
			end
			redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])
			return 1"#;
    let (_, ts, _) = self.get_queue(&qname, false)?;
    let key = self.message_zset_key(qname);
    let expires_at = ts + hidefor * 1000u64;
    let con = self.pool.get()?;
    redis::Script::new(LUA)
      .key(key)
      .key(msgid)
      .key(expires_at)
      .invoke::<()>(con.deref())?;
    Ok(expires_at)
  }

  pub fn send_message(&self, qname: &str, message: &str, delay: Option<u64>) -> RsmqResult<String> {
    let (q, ts, uid) = self.get_queue(&qname, true)?;
    let uid = uid.unwrap(); // TODO: return error here, don't panic
    let delay = delay.unwrap_or(q.delay);

    if q.maxsize != -1 && message.as_bytes().len() > q.maxsize as usize {
      let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Message is too long");
      let redis_err = RedisError::from(custom_error);
      return Err(redis_err.into());
    }
    let key = self.message_zset_key(qname);
    let qky = self.queue_hash_key(qname);
    let con = self.pool.get()?;
    redis::pipe().atomic()
      .cmd("ZADD").arg(&key).arg(ts + delay * 1000).arg(&uid).ignore()
      .cmd("HSET") .arg(&qky) .arg(&uid) .arg(message) .ignore()
      .cmd("HINCRBY") .arg(&qky) .arg("totalsent") .arg(1) .ignore()
      .query::<()>(con.deref())?;
    Ok(uid)
  }

  pub fn delete_message(&self, qname: &str, msgid: &str) -> RsmqResult<bool> {
    let key = self.message_zset_key(qname);
    let con = self.pool.get()?;
    let (delete_count, deleted_fields_count): (u32, u32) = redis::pipe()
      .atomic()
      .cmd("ZREM")
      .arg(&key)
      .arg(msgid)
      .cmd("HDEL")
      .arg(format!("{}:Q", &key))
      .arg(msgid)
      .arg(format!("{}:rc", &key))
      .arg(format!("{}:fr", &key))
      .query(con.deref())?;

    if delete_count == 1 && deleted_fields_count > 0 {
      Ok(true)
    } else {
      Ok(false)
    }
  }

  pub fn pop_message(&self, qname: &str) -> RsmqResult<Message> {
    const LUA: &'static str = r##"
      local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
			if #msg == 0 then
				return {}
			end
			redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
			local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
			local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
			local o = {msg[1], mbody, rc}
			if rc==1 then
				table.insert(o, KEYS[2])
			else			
				local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")	
				table.insert(o, fr)
			end
			redis.call("ZREM", KEYS[1], msg[1])
			redis.call("HDEL", KEYS[1] .. ":Q", msg[1], msg[1] .. ":rc", msg[1] .. ":fr")
			return o    
    "##;
    let (_, ts, _) = self.get_queue(qname, false)?;
    let key = self.message_zset_key(qname);
    let con = self.pool.get()?;
    let m: Message = redis::Script::new(LUA)
      .key(key)
      .key(ts)
      .invoke(con.deref())?;
    Ok(m)
    
  }

  pub fn receive_message(&self, qname: &str, hidefor: Option<u64>) -> RsmqResult<Message> {
    const LUA: &'static str = r##"
      local msg = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2], "LIMIT", "0", "1")
			if #msg == 0 then
				return {}
			end
			redis.call("ZADD", KEYS[1], KEYS[3], msg[1])
			redis.call("HINCRBY", KEYS[1] .. ":Q", "totalrecv", 1)
			local mbody = redis.call("HGET", KEYS[1] .. ":Q", msg[1])
			local rc = redis.call("HINCRBY", KEYS[1] .. ":Q", msg[1] .. ":rc", 1)
			local o = {msg[1], mbody, rc}
			if rc==1 then
				redis.call("HSET", KEYS[1] .. ":Q", msg[1] .. ":fr", KEYS[2])
				table.insert(o, KEYS[2])
			else
				local fr = redis.call("HGET", KEYS[1] .. ":Q", msg[1] .. ":fr")
				table.insert(o, fr)
			end
			return o
      "##;
    let (q, ts, _) = self.get_queue(&qname, false)?;
    let hidefor = hidefor.unwrap_or(q.vt);
    let key = self.message_zset_key(qname);
    let expires_at = ts + hidefor * 1000u64;
    let con = self.pool.get()?;

    let m: Message = redis::Script::new(LUA)
      .key(key)
      .key(ts)
      .key(expires_at)
      .invoke(con.deref())?;
    Ok(m)
  }

  pub fn get_queue_attributes(&self, qname: &str) -> RsmqResult<Queue> {
    // TODO: validate qname
    let con = self.pool.get()?;
    let key = self.message_zset_key(qname);
    let qkey = self.queue_hash_key(qname);
    // TODO: use transaction here to grab the time and then run the data fetch
    let (time, _): (String, u32) = redis::cmd("TIME").query(con.deref())?;
    let ts_str = format!("{}000", time);
    // [[60, 10, 1200, 5, 7, 1512492628, 1512492628], 10, 9]
    let out: ((u64, u64, i64, u64, u64, u64, u64), u64, u64) = redis::pipe().atomic()
      .cmd("HMGET").arg(qkey).arg("vt").arg("delay").arg("maxsize").arg("totalrecv").arg("totalsent").arg("created").arg("modified")
      .cmd("ZCARD").arg(&key)
      .cmd("ZCOUNT").arg(&key).arg(ts_str).arg("+inf")
      .query(con.deref())?;
    let (vt, delay, maxsize, totalrecv, totalsent, created, modified) = out.0;
    let msg_count = out.1;
    let hidden_msg_count = out.2;
    let q = Queue {
      qname:      qname.into(),
      vt:         vt,
      delay:      delay,
      maxsize:    maxsize,
      totalrecv:  totalrecv,
      totalsent:  totalsent,
      created:    created,
      modified:   modified,
      msgs:       msg_count,
      hiddenmsgs: hidden_msg_count,
    };
    Ok(q)
  }

  pub fn set_queue_attributes(&self, qname: &str, vt: Option<u64>, delay: Option<u64>, maxsize: Option<i64>) -> RsmqResult<Queue> {
    let con = self.pool.get()?;
    let qkey = self.queue_hash_key(qname);
    let mut pipe = redis::pipe();
    if vt.is_some() {
      pipe.cmd("HSET").arg(&qkey).arg("vt").arg(vt).ignore();
    }
    if delay.is_some() {
      pipe.cmd("HSET").arg(&qkey).arg("delay").arg(delay).ignore();
    }
    if maxsize.is_some() {
      pipe.cmd("HSET").arg(&qkey).arg("maxsize").arg(maxsize).ignore();
    }
    pipe.atomic().query::<()>(con.deref())?;
    let q = self.get_queue_attributes(qname)?;
    Ok(q)
  }

  fn queue_hash_key(&self, qname: &str) -> String {
    format!("{}:{}:Q", self.name_space, qname)
  }

  fn message_zset_key(&self, qname: &str) -> String {
    format!("{}:{}", self.name_space, qname)
  }
}

fn make_id_22() -> String {
  use rand::Rng;
  rand::thread_rng().gen_ascii_chars().take(22).collect()
}
