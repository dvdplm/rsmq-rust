extern crate criterion;
extern crate rsmq;

use criterion::Criterion;
use rsmq::*;

#[test]
fn criterion_benchmark() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").expect("Can't instantiate RSMQ");
  let q = Queue::new("bench-queue", 60, 0, 1200);
  rsmq.create_queue(q).expect("queue creation failed");
  static MSG_BODY: &str =
    "abcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyz";
  Criterion::default().bench_function("send message", |b| {
    b.iter(|| {
      rsmq
        .send_message("bench-queue", MSG_BODY, None)
        .expect("no, did not send that");
    })
  });

  let mut work = Vec::new();
  Criterion::default().bench_function("receive message", |b| {
    b.iter(|| {
      work.push(
        rsmq
          .receive_message("bench-queue", None)
          .expect("no, did not receive that"),
      );
    })
  });
  let qattrs = rsmq.get_queue_attributes("bench-queue");
  println!("Work to do: {}", work.len());
  println!("Queue: {:?}", qattrs);
}
