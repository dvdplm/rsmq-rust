extern crate redis;

use redis::{RedisResult, Value, RedisError};

static REDIS_NS: &str = "rsmq";
static PONG: &str = "PONG";

#[derive(Clone, Debug)]
pub struct QueueOpts {
    pub qname: String,
    pub vt: usize,
    pub delay: usize,
    pub maxsize: usize,
}

impl Default for QueueOpts {
    fn default() -> QueueOpts {
        QueueOpts {
            qname: "".into(),
            vt: 30,
            delay: 0,
            maxsize: 65536
        }
    }
}

#[derive(Debug)]
pub struct Rsmq {
    client: redis::Client,
}

impl Rsmq {
    pub fn new<T: redis::IntoConnectionInfo>(params: T) -> Result<Rsmq, RedisError> {
        let client = redis::Client::open(params)?;
        let con: redis::Connection = client.get_connection()?;
        let pong: String = redis::cmd("PING").query(&con)?;
        assert_eq!(pong, PONG);
        Ok(Rsmq { client: client })
    }
    pub fn create_queue(&self, opts: QueueOpts) -> RedisResult<Value> {
        let con = self.client.get_connection()?;
        let key = format!("{}:{}:Q", REDIS_NS, opts.qname);
        let (ts, _): (u32, u32) = redis::cmd("TIME").query(&con)?;
        // TODO: default values for the queue: vt: 30, delay: 0, maxsize: 65536
        // println!("NOW, ts: {:?}", ts);
        let res = redis::pipe()
            .atomic()
            .cmd("HSETNX").arg(&key).arg("vt").arg(opts.vt).ignore()
            .cmd("HSETNX").arg(&key).arg("delay").arg(opts.delay).ignore()
            .cmd("HSETNX").arg(&key).arg("maxsize").arg(opts.maxsize).ignore()
            .cmd("HSETNX").arg(&key).arg("created").arg(ts).ignore()
            .cmd("HSETNX").arg(&key).arg("modified").arg(ts) .ignore()
            .cmd("SADD").arg(format!("{}:QUEUES", REDIS_NS)).arg(opts.qname)
            .query::<Value>(&con)?;
        // println!("result: {:?}", res);
        Ok(res)
    }
    pub fn delete_queue(&self, opts: QueueOpts) -> RedisResult<Value> {
        let con = self.client.get_connection()?;
        let key = format!("{}:{}", REDIS_NS, opts.qname);
        redis::pipe()
            .atomic()
            .cmd("DEL").arg(format!("{}:Q", &key)).ignore() // The queue hash
            .cmd("DEL").arg(&key).ignore() // The messages zset
            .cmd("SREM").arg(format!("{}:QUEUES", REDIS_NS)).arg(opts.qname).ignore()
            .query(&con)
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
