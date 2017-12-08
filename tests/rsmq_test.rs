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
  let queue_stats_before = rsmq.get_queue_attributes("test-q").expect("fetch queue stats BEFORE failed");
  let message_id = rsmq.send_message("test-q", "fancy schmancy message", None);
  let queue_stats_after = rsmq.get_queue_attributes("test-q").expect("fetch queue stats AFTER failed");

  assert!(message_id.is_ok());
  assert_eq!(
    queue_stats_before.msgs+1,
    queue_stats_after.msgs
  )
}