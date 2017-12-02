extern crate rsmq;

use rsmq::*;

fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").expect("Can't instantiate RSMQ");
  println!("Have rsmq instance: {:?}", rsmq);

  let qopts = QueueOpts {
    qname: "my-queue".into(),
    vt: 60,
    delay: 120,
    maxsize: 3000,
  };
  rsmq.create_queue(qopts).expect("queue creation failed");
  // println!("created queue {}", qopts.qname);
  let qs = rsmq.list_queues().expect("Nope, no listing for you");
  println!("List queues: {:?}", qs);
  rsmq.delete_queue("my-queue").expect("q deletion failed");
  // println!("deleted queue {}", qopts.qname);
}
