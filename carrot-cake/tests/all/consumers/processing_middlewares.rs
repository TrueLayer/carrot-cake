use crate::helpers::{get_connection_factory, get_publisher};
use carrot_cake::consumers::hooks::pre_start::DurableQueueCreator;
use carrot_cake::consumers::{
    ConsumerGroup, HandlerError, Incoming, MessageHandler, Next, ProcessingMiddleware,
};
use carrot_cake::publishers::MessageEnvelope;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn a_middleware_can_abort_early_and_prevent_handler_execution() {
    #[derive(Clone)]
    struct Context {
        handler_was_called: Arc<Mutex<bool>>,
    }

    async fn handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        let mut guard = incoming.context.handler_was_called.lock().await;
        *guard = true;
        Ok(())
    }

    struct AbortingMiddleware;

    #[async_trait::async_trait]
    impl<C: Send + Sync + 'static> ProcessingMiddleware<C> for AbortingMiddleware {
        async fn handle<'a>(
            &'a self,
            _incoming: Incoming<'a, C>,
            _next: Next<'a, C>,
        ) -> Result<(), HandlerError> {
            // Never call the handler
            Ok(())
        }
    }

    // Arrange

    let queue_name = Uuid::new_v4().to_string();

    let context = Context {
        handler_was_called: Arc::new(Mutex::new(false)),
    };
    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .with_processing_middleware(AbortingMiddleware)
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
    handle.await.unwrap().unwrap();

    // Assert
    let handler_was_called = context.handler_was_called.lock().await;
    assert!(!*handler_was_called);
}

#[tokio::test]
async fn middlewares_are_executed_in_registration_order() {
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

    // Arrange

    let queue_name = Uuid::new_v4().to_string();

    let context = Context {
        middleware_counter: Arc::new(Mutex::new(0)),
    };
    let first_middleware = CountingMiddleware::default();
    let second_middleware = CountingMiddleware::default();
    let third_middleware = CountingMiddleware::default();
    let consumer_group = ConsumerGroup::builder(get_connection_factory(), context.clone())
        .with_processing_middleware(first_middleware.clone())
        .with_processing_middleware(second_middleware.clone())
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&queue_name)
                .with_processing_middleware(third_middleware.clone())
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
    assert_eq!(Some(0), *first_middleware.on_the_way_in.lock().await);
    assert_eq!(Some(1), *second_middleware.on_the_way_in.lock().await);
    assert_eq!(Some(2), *third_middleware.on_the_way_in.lock().await);
    // <- Handler ->
    assert_eq!(Some(4), *third_middleware.on_the_way_out.lock().await);
    assert_eq!(Some(5), *second_middleware.on_the_way_out.lock().await);
    assert_eq!(Some(6), *first_middleware.on_the_way_out.lock().await);
}

#[tokio::test]
async fn handler_middlewares_are_only_executed_for_matching_messages() {
    #[derive(Clone)]
    struct Context;

    async fn handler(_incoming: Incoming<'_, Context>) -> Result<(), HandlerError> {
        Ok(())
    }

    #[derive(Clone, Default)]
    struct SwitchMiddleware {
        has_been_executed: Arc<Mutex<bool>>,
    }

    #[async_trait::async_trait]
    impl ProcessingMiddleware<Context> for SwitchMiddleware {
        async fn handle<'a>(
            &'a self,
            incoming: Incoming<'a, Context>,
            next: Next<'a, Context>,
        ) -> Result<(), HandlerError> {
            {
                *self.has_been_executed.lock().await = true;
                // Drop lock
            }
            next.run(incoming).await
        }
    }

    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let second_queue_name = Uuid::new_v4().to_string();

    let first_middleware = SwitchMiddleware::default();
    let second_middleware = SwitchMiddleware::default();
    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_processing_middleware(first_middleware.clone())
                .with_pre_start_hook(DurableQueueCreator)
                .handler(handler),
        )
        .message_handler(
            MessageHandler::builder(&second_queue_name)
                .with_processing_middleware(second_middleware.clone())
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
            routing_key: first_queue_name,
            properties: None,
        })
        .await
        .unwrap();
    handle.await.unwrap().unwrap();

    // Assert
    assert!(*first_middleware.has_been_executed.lock().await);
    assert!(!*second_middleware.has_been_executed.lock().await);
}
