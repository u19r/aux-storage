use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use tokio_rusqlite::{Connection, Error};

#[tokio::test]
async fn given_serial_calls_when_submitted_then_worker_preserves_order() {
    let connection = Connection::open_in_memory().await.expect("open database");
    let order = Arc::new(Mutex::new(Vec::new()));

    for value in 0..3 {
        let order = Arc::clone(&order);
        connection
            .call(move |_| {
                order.lock().expect("order lock").push(value);
                Ok::<_, Error>(())
            })
            .await
            .expect("ordered call");
    }

    assert_eq!(*order.lock().expect("order lock"), vec![0, 1, 2]);
    connection.close().await.expect("close database");
}

#[tokio::test]
async fn given_many_slow_calls_when_queue_is_full_then_all_calls_complete() {
    const OPERATIONS: usize = 300;
    let connection = Connection::open_in_memory().await.expect("open database");
    let mut tasks = Vec::with_capacity(OPERATIONS);

    for _ in 0..OPERATIONS {
        let connection = connection.clone();
        tasks.push(tokio::spawn(async move {
            connection
                .call(move |_| {
                    thread::sleep(Duration::from_micros(100));
                    Ok::<_, Error>(())
                })
                .await
                .expect("queued call");
        }));
    }

    for task in tasks {
        task.await.expect("probe task");
    }
    connection.close().await.expect("close database");
}

#[tokio::test]
async fn given_closed_connection_when_called_then_call_errors_and_copy_close_succeeds() {
    let connection = Connection::open_in_memory().await.expect("open database");
    let copy = connection.clone();

    connection.close().await.expect("close database");
    let result = copy
        .call(|_| Ok::<_, Error>(()))
        .await
        .expect_err("closed call must fail");
    assert!(matches!(result, Error::ConnectionClosed));
    copy.close().await.expect("close copy");
}
