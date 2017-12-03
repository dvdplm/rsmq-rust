extern crate rsmq;

use rsmq::*;

fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").expect("Can't instantiate RSMQ");
  println!("Have rsmq instance: {:?}", rsmq);
  let qopts = QueueOpts::new("my-queue", 60, 120, 1200);
  rsmq.create_queue(qopts).expect("queue creation failed");
  // println!("created queue {}", qopts.qname);
  let qs = rsmq.list_queues().expect("Nope, no listing for you");
  println!("List queues: {:?}", qs);
  let msgid = rsmq.send_message("my-queue", "hejhopplingonsnopp", None).expect("no, did not send that");
  println!("Message ID: {}", msgid);
  let o = rsmq.change_message_visibility("my-queue", &msgid, 500);
  println!("change message visibility: {:?}", o);
  rsmq.delete_queue("my-queue").expect("q deletion failed");
  // println!("deleted queue {}", qopts.qname);
}
