use rsmq::*;
use std::time::Instant;

#[tokio::main]
async fn main() {
	let rsmq = Rsmq::new("redis://127.0.0.1/", "rsmq")
		.await
		.expect("Can't instantiate RSMQ");
	println!("[main] Have rsmq instance: {:?}", rsmq);

	let queue = Queue::new("my-queue", None, Some(5), None);
	rsmq.create_queue(queue.clone()).await.expect("queue creation failed");
	println!("[main] created queue {}", &queue.qname);

	let qs = rsmq.list_queues().await.expect("Nope, no listing for you");
	println!("[main] List queues: {:?}", qs);

	let msgid = rsmq
		.send_message("my-queue", "hejhopplingonsnopp", None)
		.await
		.expect("no, did not send that");
	println!("[main] Message ID: {:?}", msgid);
	let qattrs = rsmq.get_queue_attributes("my-queue").await.expect("error getting queue info (0)");
	println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

	// Will fail, we have a delay of 10sec on the queue so there are no messages available
	let now = Instant::now();
	println!("[main] waiting for message");
	let popped = loop {
		let res = rsmq.receive_message("my-queue", None).await;
		if res.is_ok() {
			break res
		}
	};
	println!("[main] waited for message for {:?}", Instant::now().duration_since(now));


	println!("[main] popped a message: {:?}", popped);
	let qattrs = rsmq.get_queue_attributes("my-queue").await.expect("error getting queue info (1)");
	println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

	// Set the delay to 0 on the queue
	let updated_q = rsmq
		.set_queue_attributes("my-queue", None, Some(0), None)
		.await
		.expect("could not set queue attrs");
	println!("[main] updated queue: {:?}", updated_q);

	// Send another message, this time it will not be hidden because we just changed the `delay` on the queue to 0
	let msgid = rsmq.send_message("my-queue", "not hidden", None)
		.await
		.expect("no, did not send that");
	println!("[main] Message ID: {:?}", msgid);
	let qattrs = rsmq.get_queue_attributes("my-queue").await.expect("error getting queue info (0)");
	println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

	// pop again
	let popped = rsmq.pop_message("my-queue").await;
	println!("[main] popped a message (again): {:?}", popped); // Will fail, we have a delay of 10 on the queue
	let qattrs = rsmq.get_queue_attributes("my-queue").await.expect("error getting queue info (1)");
	println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

	let o = rsmq.change_message_visibility("my-queue", &msgid, 500).await;
	println!("[main] change message visibility: {:?}", o.unwrap());

	// Will fail, there's only one message left and it's hidden
	let m = rsmq.receive_message("my-queue", None).await;
	println!("[main] reserved a message: {:?}", m);
	// Send another message, this time it will not be hidden because we just changed the `delay` on the queue to 0
	let msgid = rsmq
		.send_message("my-queue", "not hidden", None)
		.await
		.expect("no, did not send that");
	let m = rsmq.receive_message("my-queue", None).await;
	println!("[main] reserved another message: {:?}", m);

	let qattrs = rsmq.get_queue_attributes("my-queue").await.expect("error getting queue info (2)");
	println!("[main] Messages in '{}': {:?}; hidden messages: {:?}", qattrs.qname, qattrs.msgs, qattrs.hiddenmsgs);

	let qattrs = rsmq.get_queue_attributes("my-queue").await;
	println!("[main] Queue attrs: {:?}", qattrs);

	let o2 = rsmq.delete_message("my-queue", &msgid).await;
	println!("[main] delete message: {:?}", o2);

	rsmq.delete_queue("my-queue").await.expect("q deletion failed");
	println!("[main] deleted queue 'my-queue'");
}
