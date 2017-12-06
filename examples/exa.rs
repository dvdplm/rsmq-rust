extern crate rsmq;

use rsmq::*;

fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").expect("Can't instantiate RSMQ");
  println!("[main] Have rsmq instance: {:?}", rsmq);
  let qopts = Queue::new("my-queue", 60, 10, 1200);
  rsmq.create_queue(qopts).expect("queue creation failed");
  // println!("[main] created queue {}", qopts.qname);
  let qs = rsmq.list_queues().expect("Nope, no listing for you");
  println!("[main] List queues: {:?}", qs);
  let msgid = rsmq.send_message("my-queue", "hejhopplingonsnopp", None).expect("no, did not send that");
  println!("[main] Message ID: {:?}", msgid);
  // let o = rsmq.change_message_visibility("my-queue", &msgid, 500);
  // println!("[main] change message visibility: {:?}", o);
  let m = rsmq.receive_message("my-queue", None);
  println!("[main] reserved a message: {:?}", m);

  let qattrs = rsmq.get_queue_attributes("my-queue");
  println!("[main] Queue attrs: {:?}", qattrs);

  // let o2 = rsmq.delete_message("my-queue", &msgid);
  // println!("[main] delete message: {:?}", o2);
  // rsmq.delete_queue("my-queue").expect("q deletion failed");
  // // println!("[main] deleted queue {}", qopts.qname);
}
