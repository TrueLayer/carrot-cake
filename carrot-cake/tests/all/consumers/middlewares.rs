use crate::helpers::{get_connection_factory, get_publisher};
use carrot_cake::consumers::hooks::pre_start::DurableQueueCreator;
use carrot_cake::consumers::{
    ConsumerGroup, HandlerError, Incoming, MessageHandler, MessageProcessing, Next,
    ProcessingMiddleware, ProcessingOutcome, TelemetryMiddleware,
};
use carrot_cake::publishers::MessageEnvelope;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn middlewares_are_executed_in_the_expected_order() {
    // Assert the order in which processing and telemetry middlewares are executed,
    // with middlewares both at the group and the handler level
    #[derive(Clone)]
    struct Context {
        middleware_counter: Arc<Mutex<u64>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        let mut counter = incoming.context.middleware_counter.lock().await;
        *counter += 1;
        Ok(())
    }

    #[derive(Clone, Default)]
    struct CountingMiddleware {
        on_the_way_in: Arc<Mutex<Option<u64>>>,
        on_the_way_out: Arc<Mutex<Option<u64>>>,
    }

    #[async_trait::async_trait]
    impl ProcessingMiddleware<Context> for CountingMiddleware {
        async fn handle<'a>(
            &'a self,
            incoming: Incoming<'a, Context>,
            next: Next<'a, Context>,
        ) -> Result<(), HandlerError> {
            let context = incoming.context.clone();

            {
                let mut counter = context.middleware_counter.lock().await;
                *self.on_the_way_in.lock().await = Some(*counter);
                *counter += 1;
                // Drop lock
            }

            // Move forward with middleware chain execution + handler execution
            let outcome = next.run(incoming).await;

            let mut counter = context.middleware_counter.lock().await;
            *self.on_the_way_out.lock().await = Some(*counter);
            *counter += 1;

            outcome
        }
    }

    #[async_trait::async_trait]
    impl TelemetryMiddleware<Context> for CountingMiddleware {
        async fn handle<'a>(
            &'a self,
            incoming: Incoming<'a, Context>,
            next: MessageProcessing<'a, Context>,
        ) -> ProcessingOutcome {
            let context = incoming.context.clone();

            {
                let mut counter = context.middleware_counter.lock().await;
                *self.on_the_way_in.lock().await = Some(*counter);
                *counter += 1;
                // Drop lock
            }

            // Move forward with telemetry middleware chain execution
            let outcome = next.run(incoming).await;

            let mut counter = context.middleware_counter.lock().await;
            *self.on_the_way_out.lock().await = Some(*counter);
            *counter += 1;

            outcome
        }
    }

    // Arrange

    let queue_name = Uuid::new_v4().to_string();

    let context = Context {
        middleware_counter: Arc::new(Mutex::new(0)),
    };
    let first_processing = CountingMiddleware::default();
    let first_telemetry = CountingMiddleware::default();
    let second_processing = CountingMiddleware::default();
    let second_telemetry = CountingMiddleware::default();
    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .with_processing_middleware(first_processing.clone())
        .with_telemetry_middleware(first_telemetry.clone())
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&queue_name)
                .with_processing_middleware(second_processing.clone())
                .with_telemetry_middleware(second_telemetry.clone())
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
    handle.await.unwrap().unwrap();

    // Assert
    assert_eq!(Some(0), *first_telemetry.on_the_way_in.lock().await);
    assert_eq!(Some(1), *second_telemetry.on_the_way_in.lock().await);
    assert_eq!(Some(2), *first_processing.on_the_way_in.lock().await);
    assert_eq!(Some(3), *second_processing.on_the_way_in.lock().await);
    // <- Handler ->
    assert_eq!(Some(5), *second_processing.on_the_way_out.lock().await);
    assert_eq!(Some(6), *first_processing.on_the_way_out.lock().await);
    assert_eq!(Some(7), *second_telemetry.on_the_way_out.lock().await);
    assert_eq!(Some(8), *first_telemetry.on_the_way_out.lock().await);
}
