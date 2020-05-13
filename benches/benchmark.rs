use criterion::Criterion;
use rsmq::*;
use futures::executor::block_on;

#[test]
fn criterion_benchmark() {
	let rsmq = block_on(Rsmq::new("redis://127.0.0.1/", "rsmq"))
		.expect("Can't instantiate RSMQ");
	let q = Queue::new("bench-queue", Some(60), Some(0), Some(1200));
	block_on(rsmq.create_queue(q))
		.expect("queue creation failed");
	static MSG_BODY: &str =
		"abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyz";
	Criterion::default().bench_function("send message", |b| {
		b.iter(|| {
			let fut= rsmq
				.send_message("bench-queue", MSG_BODY, None);
			block_on(fut)
				.expect("no, did not send that");
		})
	});

	let mut work = Vec::new();
	Criterion::default().bench_function("receive message", |b| {
		b.iter(|| {
			let fut = rsmq
				.receive_message("bench-queue", None);
			let w = block_on(fut).expect("no, did not receive that");
			work.push(w);
		})
	});
	let qattrs = block_on(rsmq.get_queue_attributes("bench-queue"));
	println!("Work to do: {}", work.len());
	println!("Queue: {:?}", qattrs);
}
