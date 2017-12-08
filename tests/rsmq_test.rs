extern crate rsmq;

use rsmq::*;

fn setup(ns: &str) -> Rsmq {
  let rsmq = Rsmq::new("redis://127.0.0.1/", ns).expect("Can't instantiate RSMQ");
  rsmq.delete_queue("test-q").unwrap();
  let q = Queue::new("test-q", None, None, None);
  let res = rsmq.create_queue(q);
  assert!(res.is_ok());
  rsmq
}

#[test]
fn create_queue() {
  let rsmq = setup("test-ns");
  let res = rsmq.create_queue(rsmq::Queue::new("test-create-q", None, None, None));
  assert!(res.is_ok());
}

#[test]
fn list_queues() {
  let rsmq = setup("test-ns");
  rsmq.create_queue(rsmq::Queue::new("test-jobs", None, None, None)).expect("can't create queue");
  let qs = rsmq.list_queues();
  assert!(qs.is_ok());
  let qs = qs.unwrap();
  assert!(!qs.is_empty());
  assert!(qs.contains(&"test-jobs".into() ));
}

#[test]
fn delete_queue() {
  let rsmq = setup("test-ns");
  rsmq.create_queue(rsmq::Queue::new("test-delete-me", None, None, None)).expect("can't create queue");
  let qs = rsmq.list_queues().unwrap();
  assert!(qs.contains(&"test-delete-me".to_string()));

  rsmq.delete_queue("test-delete-me").expect("delete queue panicked");
  let qs = rsmq.list_queues().unwrap();
  assert!(!qs.contains(&"test-delete-me".to_string()))
}

#[test]
fn send_message() {
  let rsmq = setup("test-ns");
  let qname = "test-send-message-q";
  rsmq.delete_queue(qname).expect("no queue deleted");
  rsmq.create_queue(rsmq::Queue::new(qname, Some(0), Some(0), None)).expect("can't create queue");
  let queue_stats_before = rsmq.get_queue_attributes(qname).expect("fetch queue stats BEFORE failed");
  let message_id = rsmq.send_message(qname, "fancy schmancy message", None);
  let queue_stats_after = rsmq.get_queue_attributes(qname).expect("fetch queue stats AFTER failed");

  assert!(message_id.is_ok());
  assert_eq!(
    queue_stats_before.msgs+1,
    queue_stats_after.msgs
  )
}

#[test]
fn delete_message() {
  let rsmq = setup("test-ns");
  let qname = "test-delete-msg-q";
  rsmq.delete_queue(qname).expect("no queue deleted");
  rsmq.create_queue(rsmq::Queue::new(qname, None, None, None)).expect("can't create queue");
  
  let queue_stats_before = rsmq.get_queue_attributes(qname).expect("fetch queue stats BEFORE failed");

  let message_id = rsmq.send_message(qname, "fancy schmancy message", None);
  assert!(message_id.is_ok());

  let queue_stats_after = rsmq.get_queue_attributes(qname).expect("fetch queue stats AFTER failed");

  let deleted = rsmq.delete_message(qname, &message_id.unwrap());
  assert!(deleted.is_ok() && deleted.unwrap());
  let queue_stats_after_delete = rsmq.get_queue_attributes(qname).expect("fetch queue stats AFTER failed");
  
  assert_eq!(
    queue_stats_before.msgs+1,
    queue_stats_after.msgs
  );

  assert_eq!(
    queue_stats_after.msgs -1,
    queue_stats_after_delete.msgs
  );
}

#[test]
fn pop_message() {
  let rsmq = setup("test-ns");
  let qname = "pop-message-q";
  rsmq.delete_queue(qname).expect("no queue deleted");
  rsmq.create_queue(Queue::new(qname, None, None, None)).expect("no queue for you!");

  let mid = rsmq.send_message(qname, "poppy message", None);
  assert!(mid.is_ok());

  let queue_stats_before = rsmq.get_queue_attributes(qname).expect("fetch queue stats BEFORE failed");
  let popped = rsmq.pop_message(qname);
  let queue_stats_after = rsmq.get_queue_attributes(qname).expect("fetch queue stats AFTER failed");

  assert!(popped.is_ok());
  assert_eq!(popped.unwrap().id, mid.unwrap());
  assert_eq!(queue_stats_after.msgs, 0);
  assert_eq!(queue_stats_before.msgs, 1);
  assert_eq!(queue_stats_after.hiddenmsgs, 0);
}

#[test]
fn receive_message() {
  let rsmq = setup("test-ns");
  let qname = "receive-message-q";
  rsmq.delete_queue(qname).expect("no queue deleted");
  rsmq.create_queue(Queue::new(qname, None, None, None)).expect("no queue for you!");
  let mid = rsmq.send_message(qname, "reccy message", Some(0));
  assert!(mid.is_ok());
  std::thread::sleep(std::time::Duration::from_millis(800)); // wait for messages to become unhidden

  let queue_stats_before = rsmq.get_queue_attributes(qname).expect("fetch queue stats BEFORE failed");
  let reserved = rsmq.receive_message(qname, None);
  assert!(reserved.is_ok());
  assert_eq!(reserved.unwrap().id, mid.unwrap());
  let queue_stats_after = rsmq.get_queue_attributes(qname).expect("fetch queue stats AFTER failed");

  assert_eq!(queue_stats_before.msgs, 1);
  assert_eq!(queue_stats_after.msgs, 1); // reserving a message does not delete it

  assert_eq!(queue_stats_after.totalrecv, 1);
  assert_eq!(queue_stats_before.totalrecv, 0);

  assert_eq!(queue_stats_after.hiddenmsgs, 1); // reserving a message hides it from others for queue.vt seconds
  assert_eq!(queue_stats_before.hiddenmsgs, 0);
}