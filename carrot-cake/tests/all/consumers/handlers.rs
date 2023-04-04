use crate::{
    consumers::TempQueueCreator,
    helpers::{get_connection_factory, get_publisher},
};
use amq_protocol_types::ShortString;
use carrot_cake::{
    amqp::AMQPProperties,
    consumers::{
        hooks::pre_start::DurableQueueCreator, ConsumerGroup, HandlerError, Incoming,
        MessageHandler,
    },
    publishers::MessageEnvelope,
};
use shutdown_handler::ShutdownHandler;
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

#[tokio::test]
async fn the_consumer_group_waits_for_completion_of_in_flight_processing_before_exiting_if_exit_after_is_specified(
) {
    #[derive(Clone, Default)]
    struct Context {
        handler_completed: Arc<Mutex<bool>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        // Take a long-ish time to process the message.
        tokio::time::sleep(Duration::from_secs(10)).await;
        *incoming.context.handler_completed.lock().await = true;
        Ok(())
    }

    // Arrange
    let context = Context::default();
    let queue_name = Uuid::new_v4().to_string();

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&queue_name)
                .with_pre_start_hook(DurableQueueCreator)
                .handler(handler),
        )
        .build()
        .await
        .unwrap();
    let publisher = get_publisher().await;

    // Act
    let handle = tokio::spawn(consumer_group.run_until_stopped());
    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: queue_name,
            properties: None,
        })
        .await
        .unwrap();

    // Act
    handle.await.unwrap().unwrap();

    // Assert
    assert!(*context.handler_completed.lock().await);
}

#[tokio::test]
async fn message_id_and_timestamp_injected_when_missing() {
    #[derive(Clone, Default)]
    struct Context {
        is_timestamp_present: Arc<Mutex<bool>>,
        is_message_id_present: Arc<Mutex<bool>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        *incoming.context.is_timestamp_present.lock().await =
            incoming.message.properties.timestamp().is_some();
        *incoming.context.is_message_id_present.lock().await =
            incoming.message.properties.message_id().is_some();

        Ok(())
    }

    // Arrange
    let context = Context::default();
    let queue_name = Uuid::new_v4().to_string();

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&queue_name)
                .with_pre_start_hook(DurableQueueCreator)
                .handler(handler),
        )
        .build()
        .await
        .unwrap();
    let publisher = get_publisher().await;

    // Act
    let handle = tokio::spawn(consumer_group.run_until_stopped());
    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: queue_name,
            properties: None,
        })
        .await
        .unwrap();

    // Act
    handle.await.unwrap().unwrap();

    // Assert
    assert!(*context.is_timestamp_present.lock().await);
    assert!(*context.is_message_id_present.lock().await);
}

/// Priority + prefetch scenario.
/// Should function as described in https://www.rabbitmq.com/consumer-priority.html.
#[tokio::test]
async fn consumer_priority_and_prefetch() {
    // A list of consumer priority in order of message receipt
    let priorities_received: Arc<Mutex<Vec<i32>>> = <_>::default();
    let queue_name = Uuid::new_v4().to_string();

    let consumer_group =
        ConsumerGroup::builder(get_connection_factory(), priorities_received.clone())
            .message_handler(
                MessageHandler::builder(&queue_name)
                    .with_pre_start_hook(TempQueueCreator)
                    .with_prefetch_count(2)
                    .with_priority(i32::MAX)
                    .handler(|inc: Incoming<Mutex<Vec<i32>>>| async move {
                        eprintln!("<<< Handling message (max)");
                        inc.context.lock().await.push(i32::MAX);
                        Result::<(), HandlerError>::Ok(())
                    }),
            )
            .message_handler(
                MessageHandler::builder(&queue_name)
                    .with_pre_start_hook(TempQueueCreator)
                    .with_prefetch_count(2)
                    // implicit 0 priority
                    .handler(|inc: Incoming<Mutex<Vec<i32>>>| async move {
                        eprintln!("<<< Handling message (0)");
                        inc.context.lock().await.push(0);
                        Result::<(), HandlerError>::Ok(())
                    }),
            )
            .message_handler(
                MessageHandler::builder(&queue_name)
                    .with_pre_start_hook(TempQueueCreator)
                    .with_prefetch_count(2)
                    .with_priority(i32::MIN)
                    .handler(|inc: Incoming<Mutex<Vec<i32>>>| async move {
                        eprintln!("<<< Handling message (min)");
                        inc.context.lock().await.push(i32::MIN);
                        Result::<(), HandlerError>::Ok(())
                    }),
            )
            .build()
            .await
            .unwrap();

    tokio::spawn(consumer_group.run_until_shutdown(Arc::default()));

    // lock consumers so all messages are published & confirmed before any acks
    let lock = priorities_received.lock().await;

    // send 6 messages (2 for each consumer)
    let publisher = get_publisher().await;
    for _ in 0..6 {
        publisher
            .publish(MessageEnvelope {
                payload: "Not relevant".into(),
                exchange_name: "".into(),
                routing_key: queue_name.clone(),
                properties: None,
            })
            .await
            .unwrap();
        eprintln!(">>> Sent message");
    }

    drop(lock); // unlock consumers

    let a = Instant::now();
    while priorities_received.lock().await.len() < 6 {
        assert!(a.elapsed() < Duration::from_secs(4), "messages not handled");
        tokio::task::yield_now().await
    }

    // * messages should be sent to max-priority first until the prefetch is full
    // * then to 0-priority until the prefetch is full
    // * then to min-priority
    assert_eq!(
        *priorities_received.lock().await,
        vec![i32::MAX, i32::MAX, 0, 0, i32::MIN, i32::MIN]
    );
}

#[tokio::test]
async fn message_id_and_timestamp_not_replaced_when_provided() {
    #[derive(Clone, Default)]
    struct Context {
        timestamp: Arc<Mutex<Option<u64>>>,
        message_id: Arc<Mutex<Option<ShortString>>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        *incoming.context.timestamp.lock().await = *incoming.message.properties.timestamp();
        *incoming.context.message_id.lock().await =
            incoming.message.properties.message_id().to_owned();

        Ok(())
    }

    // Arrange
    let context = Context::default();
    let queue_name = Uuid::new_v4().to_string();
    let message_id: ShortString = Uuid::new_v4().to_string().into();
    let timestamp: u64 = SystemTime::now()
        .checked_sub(Duration::new(86400, 0))
        .expect("Could not subtract one day")
        .duration_since(UNIX_EPOCH)
        .expect("System date is before 1970")
        .as_secs();

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&queue_name)
                .with_pre_start_hook(DurableQueueCreator)
                .handler(handler),
        )
        .build()
        .await
        .unwrap();
    let publisher = get_publisher().await;

    // Act
    let handle = tokio::spawn(consumer_group.run_until_stopped());
    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: queue_name,
            properties: Some(
                AMQPProperties::default()
                    .with_message_id(message_id.clone())
                    .with_timestamp(timestamp),
            ),
        })
        .await
        .unwrap();

    // Act
    handle.await.unwrap().unwrap();

    // Assert
    assert_eq!(*context.timestamp.lock().await, Some(timestamp));
    assert_eq!(*context.message_id.lock().await, Some(message_id));
}

#[tokio::test]
async fn consumers_shut_down_on_signal() {
    #[derive(Clone, Default)]
    struct Context {
        count: Arc<RwLock<usize>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        *incoming.context.count.write().await += 1;
        Ok(())
    }

    // Arrange
    let context = Context::default();
    let queue_name1 = Uuid::new_v4().to_string();
    let queue_name2 = Uuid::new_v4().to_string();

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .message_handler(
            MessageHandler::builder(&queue_name1)
                .with_pre_start_hook(DurableQueueCreator)
                .with_prefetch_count(5)
                .handler(handler),
        )
        .message_handler(
            MessageHandler::builder(&queue_name2)
                .with_pre_start_hook(DurableQueueCreator)
                .with_prefetch_count(12)
                .handler(handler),
        )
        .build()
        .await
        .unwrap();
    let publisher = get_publisher().await;

    let shutdown = Arc::new(ShutdownHandler::new());

    // Act
    let handle = tokio::spawn(consumer_group.run_until_shutdown(shutdown.clone()));

    // this ensures that the handlers don't run until we want them to
    let guard = context.count.read().await;

    for _ in 0..20 {
        publisher
            .publish(MessageEnvelope {
                payload: "Not relevant".into(),
                exchange_name: "".into(),
                routing_key: queue_name1.clone(),
                properties: None,
            })
            .await
            .unwrap();
        publisher
            .publish(MessageEnvelope {
                payload: "Not relevant".into(),
                exchange_name: "".into(),
                routing_key: queue_name2.clone(),
                properties: None,
            })
            .await
            .unwrap();
    }

    shutdown.shutdown(); // shutdown.

    drop(guard); // allow handlers to run. they should all drain out before the consumers exit gracefully

    handle.await.unwrap().unwrap();

    // Assert
    assert_eq!(*context.count.read().await, 5 + 12);
}
