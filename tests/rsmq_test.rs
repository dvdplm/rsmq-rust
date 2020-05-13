use rsmq::*;

async fn setup(ns: &str) -> Rsmq {
	let rsmq = Rsmq::new("redis://127.0.0.1/", ns).await.expect("Can't instantiate RSMQ");
	rsmq.delete_queue("test-q").await.unwrap();
	let q = Queue::new("test-q", None, None, None);
	let res = rsmq.create_queue(q).await;
	assert!(res.is_ok());
	rsmq
}

#[tokio::test]
async fn create_queue() {
	let rsmq = setup("test-ns").await;
	let res = rsmq.create_queue(rsmq::Queue::new("test-create-q", None, None, None)).await;
	assert!(res.is_ok());
}

#[tokio::test]
async fn list_queues() {
	let rsmq = setup("test-ns").await;
	rsmq.create_queue(rsmq::Queue::new("test-jobs", None, None, None)).await.expect("can't create queue");
	let qs = rsmq.list_queues().await;
	assert!(qs.is_ok());
	let qs = qs.unwrap();
	assert!(!qs.is_empty());
	assert!(qs.contains(&"test-jobs".into()));
}

#[tokio::test]
async fn delete_queue() {
	let rsmq = setup("test-ns").await;
	rsmq.create_queue(rsmq::Queue::new("test-delete-me", None, None, None)).await.expect("can't create queue");
	let qs = rsmq.list_queues().await.unwrap();
	assert!(qs.contains(&"test-delete-me".to_string()));

	rsmq.delete_queue("test-delete-me").await.expect("delete queue panicked");
	let qs = rsmq.list_queues().await.unwrap();
	assert!(!qs.contains(&"test-delete-me".to_string()))
}

#[tokio::test]
async fn send_message() {
	let rsmq = setup("test-ns").await;
	let qname = "test-send-message-q";
	rsmq.delete_queue(qname).await.expect("no queue deleted");
	rsmq.create_queue(rsmq::Queue::new(qname, Some(0), Some(0), None)).await.expect("can't create queue");
	let queue_stats_before = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats BEFORE failed");
	let message_id = rsmq.send_message(qname, "fancy schmancy message", None).await;
	let queue_stats_after = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats AFTER failed");

	assert!(message_id.is_ok());
	assert_eq!(
		queue_stats_before.msgs + 1,
		queue_stats_after.msgs
	)
}

#[tokio::test]
async fn delete_message() {
	let rsmq = setup("test-ns").await;
	let qname = "test-delete-msg-q";
	rsmq.delete_queue(qname).await.expect("no queue deleted");
	rsmq.create_queue(rsmq::Queue::new(qname, None, None, None)).await.expect("can't create queue");

	let queue_stats_before = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats BEFORE failed");

	let message_id = rsmq.send_message(qname, "fancy schmancy message", None).await;
	assert!(message_id.is_ok());

	let queue_stats_after = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats AFTER failed");

	let deleted = rsmq.delete_message(qname, &message_id.unwrap()).await;
	assert!(deleted.is_ok() && deleted.unwrap());
	let queue_stats_after_delete = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats AFTER failed");

	assert_eq!(
		queue_stats_before.msgs + 1,
		queue_stats_after.msgs
	);

	assert_eq!(
		queue_stats_after.msgs - 1,
		queue_stats_after_delete.msgs
	);
}

#[tokio::test]
async fn pop_message() {
	let rsmq = setup("test-ns").await;
	let qname = "pop-message-q";
	rsmq.delete_queue(qname).await.expect("no queue deleted");
	rsmq.create_queue(Queue::new(qname, None, None, None)).await.expect("no queue for you!");

	let msg_id = rsmq.send_message(qname, "poppy message", None).await;
	assert!(msg_id.is_ok());

	let queue_stats_before = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats BEFORE failed");
	let popped = rsmq.pop_message(qname).await;
	let queue_stats_after = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats AFTER failed");

	assert!(popped.is_ok());
	assert_eq!(popped.unwrap().id, msg_id.unwrap());
	assert_eq!(queue_stats_after.msgs, 0);
	assert_eq!(queue_stats_before.msgs, 1);
	assert_eq!(queue_stats_after.hiddenmsgs, 0);
}

#[tokio::test]
async fn receive_message() {
	let rsmq = setup("test-ns").await;
	let qname = "receive-message-q";
	rsmq.delete_queue(qname).await.expect("no queue deleted");
	rsmq.create_queue(Queue::new(qname, None, None, None)).await.expect("no queue for you!");
	let msg_id = rsmq.send_message(qname, "a message to receive", Some(0)).await;
	assert!(msg_id.is_ok());
	std::thread::sleep(std::time::Duration::from_millis(1000)); // wait for messages to become unhidden

	let queue_stats_before = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats BEFORE failed");
	let reserved = rsmq.receive_message(qname, None).await;
	assert!(reserved.is_ok());
	assert_eq!(reserved.unwrap().id, msg_id.unwrap());
	let queue_stats_after = rsmq.get_queue_attributes(qname).await.expect("fetch queue stats AFTER failed");

	assert_eq!(queue_stats_before.msgs, 1);
	assert_eq!(queue_stats_after.msgs, 1); // reserving a message does not delete it

	assert_eq!(queue_stats_after.totalrecv, 1);
	assert_eq!(queue_stats_before.totalrecv, 0);

	assert_eq!(queue_stats_after.hiddenmsgs, 1); // reserving a message hides it from others for queue.vt seconds
	assert_eq!(queue_stats_before.hiddenmsgs, 0);
}