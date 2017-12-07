extern crate num_bigint;
extern crate r2d2;
extern crate r2d2_redis_patch as r2d2_redis;
extern crate rand;
extern crate redis;

use std::default::Default;
use std::error::Error;
use std::ops::Deref;
use num_bigint::BigUint;
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
  pub fn new(qname: &str, vt: u64, delay: u64, maxsize: i64) -> Queue {
    let mut q = Queue {
      ..Default::default()
    };
    q.qname = qname.into();
    q.vt = vt;
    q.delay = delay;
    q.maxsize = maxsize;
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
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "{:?}", self.pool) }
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
    let key = format!("{}:{}:Q", self.name_space, opts.qname);
    let (ts, _): (u32, u32) = redis::cmd("TIME").query(con.deref())?;
    let (res,): (u8,) = redis::pipe()
      .atomic()
      .cmd("HSETNX")
      .arg(&key)
      .arg("vt")
      .arg(opts.vt)
      .ignore()
      .cmd("HSETNX")
      .arg(&key)
      .arg("delay")
      .arg(opts.delay)
      .ignore()
      .cmd("HSETNX")
      .arg(&key)
      .arg("maxsize")
      .arg(opts.maxsize)
      .ignore()
      .cmd("HSETNX")
      .arg(&key)
      .arg("created")
      .arg(ts)
      .ignore()
      .cmd("HSETNX")
      .arg(&key)
      .arg("modified")
      .arg(ts)
      .ignore()
      .cmd("SADD")
      .arg(format!("{}:QUEUES", self.name_space))
      .arg(opts.qname)
      .query(con.deref())?;
    Ok(res)
  }

  pub fn delete_queue(&self, qname: &str) -> RsmqResult<Value> {
    let con = self.pool.get()?;
    let key = format!("{}:{}", self.name_space, qname);
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
    let qkey = format!("{}:{}:Q", self.name_space, qname);
    let ((vt, delay, maxsize), (secs, micros)): ((u64, u64, i64), (u64, u64)) = redis::pipe()
      .atomic()
      .cmd("HMGET")
      .arg(qkey)
      .arg("vt")
      .arg("delay")
      .arg("maxsize")
      .cmd("TIME")
      .query(con.deref())?;

    let ts = (secs * 1_000_000 + micros) / 1_000; // Epoch time in milliseconds
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
      let ts_str = format!("{}{:06}", secs, micros); // This is a bit nuts; double check JS behavior
      let ts_rad36 = BigUint::parse_bytes(ts_str.as_bytes(), 10)
        .unwrap()
        .to_str_radix(36);
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
    let queue_key = format!("{}:{}", self.name_space, qname);
    let expires_at = ts + hidefor * 1000u64;
    let con = self.pool.get()?;
    redis::Script::new(LUA)
      .key(queue_key)
      .key(msgid)
      .key(expires_at)
      .invoke::<()>(con.deref())?;
    Ok(expires_at)
  }

  pub fn send_message(&self, qname: &str, message: &str, delay: Option<u64>) -> RsmqResult<String> {
    let (q, ts, uid) = self.get_queue(&qname, true)?;
    let uid = uid.unwrap();
    let delay = delay.unwrap_or(q.delay);

    if q.maxsize != -1 && message.len() > q.maxsize as usize {
      // TODO: len() is utf8 chars; maxsize is bytes
      let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Message is too long");
      let redis_err = RedisError::from(custom_error);
      return Err(redis_err.into());
    }
    let key = format!("{}:{}", self.name_space, qname);
    let qky = format!("{}:Q", key);
    let con = self.pool.get()?;
    redis::pipe()
      .atomic()
      .cmd("ZADD")
      .arg(&key)
      .arg(ts + delay * 1000)
      .arg(&uid)
      .ignore()
      .cmd("HSET")
      .arg(&qky)
      .arg(&uid)
      .arg(message)
      .ignore()
      .cmd("HINCRBY")
      .arg(&qky)
      .arg("totalsent")
      .arg(1)
      .ignore()
      .query::<()>(con.deref())?;
    Ok(uid)
  }

  pub fn delete_message(&self, qname: &str, msgid: &str) -> RsmqResult<bool> {
    let key = format!("{}:{}", self.name_space, qname);
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
    let qky = format!("{}:{}", self.name_space, qname);
    let expires_at = ts + hidefor * 1000u64;
    let con = self.pool.get()?;

    let m: Message = redis::Script::new(LUA)
      .key(qky)
      .key(ts)
      .key(expires_at)
      .invoke(con.deref())?;
    Ok(m)
  }

  pub fn get_queue_attributes(&self, qname: &str) -> RsmqResult<Queue> {
    // TODO: validate qname
    let key = format!("{}:{}", self.name_space, qname);
    let qkey = format!("{}:{}:Q", self.name_space, qname);
    let con = self.pool.get()?;
    // TODO: use transaction here to grab the time and then run the data fetch
    let (time, _): (String, u32) = redis::cmd("TIME").query(con.deref())?;
    let ts_str = format!("{}000", time);
    // [[60, 10, 1200, 5, 7, 1512492628, 1512492628], 10, 9]
    let out: ((u64, u64, i64, u64, u64, u64, u64), u64, u64) = redis::pipe()
      .atomic()
      .cmd("HMGET")
      .arg(qkey)
      .arg("vt")
      .arg("delay")
      .arg("maxsize")
      .arg("totalrecv")
      .arg("totalsent")
      .arg("created")
      .arg("modified")
      .cmd("ZCARD")
      .arg(&key)
      .cmd("ZCOUNT")
      .arg(&key)
      .arg(ts_str)
      .arg("+inf")
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
}

fn make_id_22() -> String {
  use rand::Rng;
  rand::thread_rng().gen_ascii_chars().take(22).collect()
}
