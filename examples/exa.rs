extern crate rsmq;

use rsmq::*;

fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").expect("Can't instantiate RSMQ");
  println!("Have rsmq instance: {:?}", rsmq);

  let qopts = QueueOpts {
    qname: "ha".into(),
    vt: 60,
    delay: 120,
    maxsize: 3000,
  };
  rsmq.create_queue(qopts.clone()).expect("q creation failed");
  println!("created queue {}", qopts.qname);
  rsmq.delete_queue(rsmq::QueueOpts{qname: "ha".into(), .. Default::default()}).expect("q deletion failed");
  println!("deleted queue {}", qopts.qname);

}
