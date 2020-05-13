use failure::{Error, format_err};
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use std::{default::Default, ops::DerefMut};
use redis::{from_redis_value, RedisError, RedisResult, Value, ErrorKind as RedisErrorKind};

#[derive(Clone, Debug)]
pub struct Queue {
	pub qname: String,
	pub vt: u64,
	pub delay: u64,
	pub maxsize: i64,
	pub totalrecv: u64,
	pub totalsent: u64,
	pub created: u64,
	pub modified: u64,
	// current message count
	pub msgs: u64,
	// hidden, aka "in-flight" messages + delayed messages
	pub hiddenmsgs: u64,
}

impl Queue {
	pub fn new(qname: &str, vt: Option<u64>, delay: Option<u64>, maxsize: Option<i64>) -> Queue {
		let mut q = Queue { ..Default::default() };
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
			qname: "".into(),
			vt: 30,
			delay: 0,
			maxsize: 65536,
			totalrecv: 0,
			totalsent: 0,
			created: 0,
			modified: 0,
			msgs: 0,
			hiddenmsgs: 0,
		}
	}
}

#[derive(Clone, Debug)]
pub struct Message {
	pub id: String,
	pub message: String,
	pub rc: u64,
	// Receive count
	pub fr: u64,
	// First receive time
	pub sent: u64,
}

impl Message {
	pub fn new() -> Message {
		Message {
			id: "".into(),
			message: "".into(),
			sent: 0,
			fr: 0,
			rc: 0,
		}
	}
}

impl redis::FromRedisValue for Message {
	fn from_redis_value(v: &Value) -> RedisResult<Message> {
		match *v {
			Value::Bulk(ref items) => {
				if items.len() == 0 {
					return Err(RedisError::from((RedisErrorKind::TryAgain, "No messages to receive")));
				}
				let mut m = Message::new();
				m.id = from_redis_value(&items[0])?;
				m.message = from_redis_value(&items[1])?;
				m.rc = from_redis_value(&items[2])?;
				m.fr = from_redis_value(&items[3])?;
				m.sent = match u64::from_str_radix(&m.id[0..10], 36) {
					Ok(ts) => ts,
					Err(e) => return Err(RedisError::from((
						RedisErrorKind::TypeError,
						"timestamp parsing error",
						format!("Could not convert '{:?}' to a timestamp. Error: {}", &m.id[0..10], e)
					)))
				};
				Ok(m)
			}
			_ => Err(RedisError::from((RedisErrorKind::IoError, "Redis did not return a Value::Bulk"))),
		}
	}
}

pub struct Rsmq {
	pool: Pool<RedisConnectionManager>,
	name_space: String,
}

impl std::fmt::Debug for Rsmq {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(f, "redis namespace: {}, {:?}", self.name_space, self.pool)
	}
}

impl Rsmq {
	pub async fn new<T: redis::IntoConnectionInfo>(params: T, name_space: &str) -> Result<Rsmq, Error> {
		let manager = RedisConnectionManager::new(params)?;
		let pool = bb8::Pool::builder().build(manager).await?;

		let name_space = if name_space != "" {
			name_space.into()
		} else {
			"rsmq".into()
		};

		Ok(Rsmq { pool, name_space })
	}

	pub async fn create_queue(&self, opts: Queue) -> Result<u8, Error> {
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let qky = self.queue_hash_key(&opts.qname);
		let (ts, _): (u32, u32) = redis::cmd("TIME").query_async(con).await?;
		let (res, ): (u8, ) = redis::pipe()
			.atomic()
			.cmd("HSETNX").arg(&qky).arg("vt").arg(opts.vt).ignore()
			.cmd("HSETNX").arg(&qky).arg("delay").arg(opts.delay).ignore()
			.cmd("HSETNX").arg(&qky).arg("maxsize").arg(opts.maxsize).ignore()
			.cmd("HSETNX").arg(&qky).arg("totalrecv").arg(0).ignore()
			.cmd("HSETNX").arg(&qky).arg("totalsent").arg(0).ignore()
			.cmd("HSETNX").arg(&qky).arg("created").arg(ts).ignore()
			.cmd("HSETNX").arg(&qky).arg("modified").arg(ts).ignore()
			.cmd("SADD").arg(format!("{}:QUEUES", self.name_space)).arg(opts.qname)
			.query_async(con)
			.await?;
		Ok(res)
	}

	pub async fn delete_queue(&self, qname: &str) -> Result<Value, Error> {
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let key = self.message_zset_key(qname);
		redis::pipe()
			.atomic()
			.cmd("DEL").arg(format!("{}:Q", &key)).ignore() // The queue hash
			.cmd("DEL").arg(&key).ignore() // The messages zset
			.cmd("SREM").arg(format!("{}:QUEUES", self.name_space)).arg(qname).ignore()
			.query_async(con)
			.await
			.map_err(|e| e.into())
	}

	pub async fn list_queues(&self) -> Result<Vec<String>, Error> {
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let key = format!("{}:QUEUES", self.name_space);
		redis::cmd("SMEMBERS")
			.arg(key)
			.query_async(con)
			.await
			.map_err(|e| e.into())
	}

	async fn get_queue(&self, qname: &str, set_uid: bool) -> Result<(Queue, u64, Option<String>), Error> {
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let qkey = self.queue_hash_key(qname);
		let ((vt, delay, maxsize), (secs, micros)): ((u64, u64, i64), (u64, u64)) = redis::pipe()
			.atomic()
			.cmd("HMGET").arg(qkey).arg("vt").arg("delay").arg("maxsize")
			.cmd("TIME")
			.query_async(con)
			.await?;

		let ts_micros = secs * 1_000_000 + micros;
		let ts = ts_micros / 1_000; // Epoch time in milliseconds
		let q = Queue {
			qname: qname.into(),
			vt,
			delay,
			maxsize,
			..Default::default()
		};
		// This is a bit crazy. The JS version calls getQueue with the `set_uid` set to `true` only from `sendMessage`
		// where it is used to write the timestamp+random stuff that constituates the (sort)key. This is just a port of
		// that behavior. I don't understand why it is baked in with the queue attrib fetch.
		let uid = if set_uid {
			let ts_rad36 = radix::RadixNum::from(ts_micros).with_radix(36).unwrap().as_str().to_lowercase().to_string();
			// TODO: make this work
			// let ts_rad36 = radix::RadixNum::from(ts_micros).with_radix(36)?.as_str().to_lowercase().to_string();
			Some(ts_rad36 + &make_id_22())
		} else {
			None
		};
		Ok((q, ts, uid))
	}

	pub async fn change_message_visibility(&self, qname: &str, msgid: &str, hidefor: u64) -> Result<u64, Error> {
		const LUA: &'static str = r#"
            local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])
			if not msg then
				return 0
			end
			redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])
			return 1"#;
		let (_, ts, _) = self.get_queue(&qname, false).await?;
		let key = self.message_zset_key(qname);
		let expires_at = ts + hidefor * 1000u64;
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		redis::Script::new(LUA)
			.key(key)
			.key(msgid)
			.key(expires_at)
			.invoke_async::<_, ()>(con)
			.await?;
		Ok(expires_at)
	}

	pub async fn send_message(&self, qname: &str, message: &str, delay: Option<u64>) -> Result<String, Error> {
		let (q, ts, uid) = self.get_queue(&qname, true).await?;
		let uid = uid.ok_or(format_err!("Did not get a proper uid back from Redis"))?;
		let delay = delay.unwrap_or(q.delay);

		if q.maxsize != -1 && message.as_bytes().len() > q.maxsize as usize {
			let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Message is too long");
			let redis_err = RedisError::from(custom_error);
			return Err(redis_err.into());
		}
		let key = self.message_zset_key(qname);
		let qky = self.queue_hash_key(qname);
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		redis::pipe().atomic()
			.cmd("ZADD").arg(&key).arg(ts + delay * 1000).arg(&uid).ignore()
			.cmd("HSET").arg(&qky).arg(&uid).arg(message).ignore()
			.cmd("HINCRBY").arg(&qky).arg("totalsent").arg(1).ignore()
			.query_async::<_, ()>(con)
			.await?;
		Ok(uid)
	}

	pub async fn delete_message(&self, qname: &str, msgid: &str) -> Result<bool, Error> {
		let key = self.message_zset_key(qname);
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
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
			.query_async(con)
			.await?;

		if delete_count == 1 && deleted_fields_count > 0 {
			Ok(true)
		} else {
			Ok(false)
		}
	}

	pub async fn pop_message(&self, qname: &str) -> Result<Message, Error> {
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
		let (_, ts, _) = self.get_queue(qname, false).await?;
		let key = self.message_zset_key(qname);
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let m: Message = redis::Script::new(LUA)
			.key(key)
			.key(ts)
			.invoke_async(con)
			.await?;
		Ok(m)
	}

	pub async fn receive_message(&self, qname: &str, hidefor: Option<u64>) -> Result<Message, Error> {
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
		let (q, ts, _) = self.get_queue(&qname, false).await?;
		let hidefor = hidefor.unwrap_or(q.vt);
		let key = self.message_zset_key(qname);
		let expires_at = ts + hidefor * 1000u64;
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;

		let m: Message = redis::Script::new(LUA)
			.key(key)
			.key(ts)
			.key(expires_at)
			.invoke_async(con)
			.await?;
		Ok(m)
	}

	pub async fn get_queue_attributes(&self, qname: &str) -> Result<Queue, Error> {
		// TODO: validate qname
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
		let key = self.message_zset_key(qname);
		let qkey = self.queue_hash_key(qname);
		// TODO: use transaction here to grab the time and then run the data fetch
		let (time, _): (String, u32) = redis::cmd("TIME")
			.query_async(con)
			.await?;
		let ts_str = format!("{}000", time);
		// [[60, 10, 1200, 5, 7, 1512492628, 1512492628], 10, 9]
		let out: ((u64, u64, i64, u64, u64, u64, u64), u64, u64) = redis::pipe().atomic()
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
			.query_async(con)
			.await?;

		let (vt, delay, maxsize, totalrecv, totalsent, created, modified) = out.0;
		let msgs = out.1;
		let hiddenmsgs = out.2;
		let q = Queue {
			qname: qname.into(),
			vt,
			delay,
			maxsize,
			totalrecv,
			totalsent,
			created,
			modified,
			msgs,
			hiddenmsgs,
		};
		Ok(q)
	}

	pub async fn set_queue_attributes(
		&self,
		qname: &str,
		vt: Option<u64>,
		delay: Option<u64>,
		maxsize: Option<i64>,
	) -> Result<Queue, Error> {
		let con = self.pool.get()
			.await?
			.as_mut()
			.ok_or_else(|| RedisError::from((RedisErrorKind::IoError, "Unable to acquire connection")))?;
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
		pipe.atomic().query_async::<_, ()>(con).await?;
		let q = self.get_queue_attributes(qname).await?;
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
	use rand::{Rng, distributions::Alphanumeric};
	rand::thread_rng()
		.sample_iter(&Alphanumeric)
		.take(22)
		.collect::<String>()
}
