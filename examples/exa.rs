extern crate rsmq;

use rsmq::*;

fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/", "rsmq").expect("Can't instantiate RSMQ");
  println!("[main] Have rsmq instance: {:?}", rsmq);

  let queue = Queue::new("my-queue", 60, 10, 1200);
  rsmq.create_queue(queue.clone()).expect("queue creation failed");
  println!("[main] created queue {}", &queue.qname);
  
  let qs = rsmq.list_queues().expect("Nope, no listing for you");
  println!("[main] List queues: {:?}", qs);
  
  let msgid = rsmq.send_message("my-queue", "hejhopplingonsnopp", None).expect("no, did not send that");
  println!("[main] Message ID: {:?}", msgid);
  let qattrs = rsmq.get_queue_attributes("my-queue").expect("error getting queue info (0)");
  println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);
  
  // Will fail, we have a delay of 10sec on the queue so there are no messages available
  let popped = rsmq.pop_message("my-queue");
  println!("[main] popped a message: {:?}", popped);
  let qattrs = rsmq.get_queue_attributes("my-queue").expect("error getting queue info (1)");
  println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);
  
  // Set the delay to 0 on the queue
  let updated_q = rsmq.set_queue_attributes("my-queue", None, Some(0), None).expect("could not set queue attrs");
  println!("[main] updated queue: {:?}", updated_q);

  // Send another message, this time it will not be hidden because we just changed the `delay` on the queue to 0
  let msgid = rsmq.send_message("my-queue", "not hidden", None).expect("no, did not send that");
  println!("[main] Message ID: {:?}", msgid);
  let qattrs = rsmq.get_queue_attributes("my-queue").expect("error getting queue info (0)");
  println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

  // pop again
  let popped = rsmq.pop_message("my-queue");
  println!("[main] popped a message (again): {:?}", popped); // Will fail, we have a delay of 10 on the queue
  let qattrs = rsmq.get_queue_attributes("my-queue").expect("error getting queue info (1)");
  println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

  let o = rsmq.change_message_visibility("my-queue", &msgid, 500);
  println!("[main] change message visibility: {:?}", o.unwrap());
  
  // Will fail, there's only one message left and it's hidden
  let m = rsmq.receive_message("my-queue", None);
  println!("[main] reserved a message: {:?}", m);
  // Send another message, this time it will not be hidden because we just changed the `delay` on the queue to 0
  let msgid = rsmq.send_message("my-queue", "not hidden", None).expect("no, did not send that");
  let m = rsmq.receive_message("my-queue", None);
  println!("[main] reserved another message: {:?}", m);
  
  let qattrs = rsmq.get_queue_attributes("my-queue").expect("error getting queue info (2)");
  println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

  let qattrs = rsmq.get_queue_attributes("my-queue");
  println!("[main] Queue attrs: {:?}", qattrs);

  let o2 = rsmq.delete_message("my-queue", &msgid);
  println!("[main] delete message: {:?}", o2);

  rsmq.delete_queue("my-queue").expect("q deletion failed");
  println!("[main] deleted queue 'my-queue'");
}
