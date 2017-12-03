extern crate redis;
extern crate num_bigint;
extern crate rand;

use num_bigint::BigUint;
use redis::{RedisResult, Value, RedisError, from_redis_value};

const REDIS_NS: &str = "rsmq";
const PONG: &str = "PONG";

#[derive(Clone, Debug)]
pub struct QueueOpts {
    uid: String,
    pub qname: String,
    pub vt: u64,
    pub delay: u64,
    pub maxsize: i64,
    ts: u64,
}

impl QueueOpts {
    pub fn new(qname: &str, vt: u64, delay: u64, maxsize: i64) -> QueueOpts {
        let mut q = QueueOpts {..Default::default()};
        q.qname = qname.into();
        q.vt = vt;
        q.delay = delay;
        q.maxsize = maxsize;
        q
    }
}

impl Default for QueueOpts {
    fn default() -> QueueOpts {
        QueueOpts {
            uid: "".into(), // I think this is what becomes the *message* ID. It is stored on the queue in memory for some to me unknown reason
            qname: "".into(),
            vt: 30,
            delay: 0,
            maxsize: 65536,
            ts: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    pub id: String,
    pub message: String,
    pub rc: u64, // Receive count
    pub fr: u64, // First receive time
    pub sent: u64,
}

impl Message {
    pub fn new() -> Message {
        Message {
            id: "".into(), message: "".into(), sent:0, fr:0, rc:0
        }
    }
}
impl redis::FromRedisValue for Message {
    fn from_redis_value(v: &Value) -> RedisResult<Message> {
        match *v {
            Value::Bulk(ref items) => {
                let mut m = Message::new();
                m.id = from_redis_value(&items[0])?;
                m.message = from_redis_value(&items[1])?;
                m.rc = from_redis_value(&items[2])?;
                m.fr = from_redis_value(&items[3])?;
                Ok(m)
            },
            _ => {
                // println!("Oh boy, what even are you? {:?}", v);
                // TODO: figure out how to return something sensible here
                let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Redis did not return a bulk");
                let redis_err = RedisError::from(custom_error);
                return Err(redis_err)
            }
        }
    }
}

// #[derive(Debug)]
pub struct Rsmq {
    client: redis::Client,
    con: redis::Connection,
}

impl std::fmt::Debug for Rsmq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.client)
    }
}

impl Rsmq {
    pub fn new<T: redis::IntoConnectionInfo>(params: T) -> Result<Rsmq, RedisError> {
        let client = redis::Client::open(params)?;
        let con: redis::Connection = client.get_connection()?;
        let pong: String = redis::cmd("PING").query(&con)?;
        assert_eq!(pong, PONG);
        Ok(Rsmq { client: client, con: con })
    }

    pub fn create_queue(&self, opts: QueueOpts) -> RedisResult<Value> {
        let con = self.client.get_connection()?;
        let key = format!("{}:{}:Q", REDIS_NS, opts.qname);
        let (ts, _): (u32, u32) = redis::cmd("TIME").query(&con)?;
        let res = redis::pipe()
            .atomic()
            .cmd("HSETNX").arg(&key).arg("vt").arg(opts.vt).ignore()
            .cmd("HSETNX").arg(&key).arg("delay").arg(opts.delay).ignore()
            .cmd("HSETNX").arg(&key).arg("maxsize").arg(opts.maxsize).ignore()
            .cmd("HSETNX").arg(&key).arg("created").arg(ts).ignore()
            .cmd("HSETNX").arg(&key).arg("modified").arg(ts) .ignore()
            .cmd("SADD").arg(format!("{}:QUEUES", REDIS_NS)).arg(opts.qname)
            .query::<Value>(&con)?;
        Ok(res)
    }

    pub fn delete_queue(&self, qname: &str) -> RedisResult<Value> {
        let con = self.client.get_connection()?;
        let key = format!("{}:{}", REDIS_NS, qname);
        redis::pipe()
            .atomic()
            .cmd("DEL").arg(format!("{}:Q", &key)).ignore() // The queue hash
            .cmd("DEL").arg(&key).ignore() // The messages zset
            .cmd("SREM").arg(format!("{}:QUEUES", REDIS_NS)).arg(qname).ignore()
            .query(&con)
    }

    pub fn list_queues(&self) -> RedisResult<Vec<String>> {
        let con = self.client.get_connection()?;
        let key = format!("{}:QUEUES", REDIS_NS);
        redis::cmd("SMEMBERS").arg(key).query(&con)
    }

    fn get_queue(&self, qname: &str, set_uid: bool) -> RedisResult<QueueOpts> {
        let qkey = format!("{}:{}:Q", REDIS_NS, qname);
        let out: Vec<Vec<u64>> = redis::pipe().atomic()
            .cmd("HMGET").arg(qkey).arg("vt").arg("delay").arg("maxsize")
            .cmd("TIME")
            .query(&self.con)?;
        let qattrs = &out[0];
        let secs = out[1][0];
        let micros = out[1][1];
        let ts = (secs * 1_000_000 + micros)/1_000; // Epoch time in milliseconds
        let mut q = QueueOpts {
            qname: qname.into(),
            uid: "".into(),
            vt: qattrs[0],
            delay: qattrs[1],
            maxsize: qattrs[2] as i64,
            ts: ts,
        };
        if set_uid {
            let ts_str = format!("{}{:06}", secs, micros);
            let ts_rad36 = BigUint::parse_bytes(ts_str.as_bytes(), 10).unwrap().to_str_radix(36);
            q.uid = ts_rad36 + &make_id_22()
        }   
        
        Ok(q)
    }

    pub fn change_message_visibility(&self, qname: &str, msgid: &str, hidefor: u64) -> Result<u64, RedisError> {
        const LUA : &'static str = r#"
            local msg = redis.call("ZSCORE", KEYS[1], KEYS[2])
			if not msg then
				return 0
			end
			redis.call("ZADD", KEYS[1], KEYS[3], KEYS[2])
			return 1"#;
        let q = self.get_queue(&qname, false)?;
        let queue_key = format!("{}:{}", REDIS_NS, qname);
        let expires_at = q.ts + hidefor * 1000u64;
        redis::Script::new(LUA)
            .key(queue_key)
            .key(msgid)
            .key(expires_at)
            .invoke::<()>(&self.con)?;
        Ok(expires_at)
    }

    pub fn send_message(&self, qname: &str, message: &str, delay: Option<u64>) -> Result<String, RedisError> {
        let q = self.get_queue(qname, true)?;
        let delay = delay.unwrap_or(q.delay);

        if q.maxsize != -1 && message.len() > q.maxsize as usize { // TODO: len() is utf8 chars; maxsize is bytes
            let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "Message is too long");
            let redis_err = RedisError::from(custom_error);
            return Err(redis_err)
        }
        let key = format!("{}:{}", REDIS_NS, qname);
        let qky = format!("{}:Q", key);
        redis::pipe().atomic()
            .cmd("ZADD").arg(&key).arg(q.ts + delay * 1000).arg(&q.uid).ignore()
            .cmd("HSET").arg(&qky).arg(&q.uid).arg(message).ignore()
            .cmd("HINCRBY").arg(&qky).arg("totalsent").arg(1).ignore()
            .query::<()>(&self.con)?;
        Ok(q.uid)
    }

    pub fn delete_message(&self, qname: &str, msgid: &str) -> Result<u64, RedisError> {
        let key = format!("{}:{}", REDIS_NS, qname);
        let res : Vec<u64> = redis::pipe().atomic()
            .cmd("ZREM").arg(&key).arg(msgid)
            .cmd("HDEL").arg(format!("{}:Q", &key)).arg(msgid).arg(format!("{}:rc", &key)).arg(format!("{}:fr", &key))
            .query(&self.con)?;

        if res[0] == 1 && res[1] > 0 {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    pub fn receive_message(&self, qname: &str, hidefor: Option<u64>) -> Result<Message, RedisError> {
        const LUA : &'static str = r##"
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
        let q = self.get_queue(qname, false)?;
        let hidefor = hidefor.unwrap_or(q.vt);
        let qky = format!("{}:{}", REDIS_NS, qname);
        let expires_at = q.ts + hidefor * 1000u64;
        
        let m : Message = redis::Script::new(LUA)
            .key(qky)
            .key(q.ts)
            .key(expires_at)
            .invoke(&self.con)?;
        Ok(m)
    }
}

fn make_id_22() -> String {
    use rand::Rng;
    rand::thread_rng().gen_ascii_chars().take(22).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
