use crate::helpers::{get_connection_factory, get_publisher};
use amq_protocol_types::FieldTable;
use anyhow::Error;
use async_trait::async_trait;
use carrot_cake::{
    amqp::Channel,
    consumers::{
        hooks::{
            pre_start::DeclareDurableQueue,
            transient_error::{AlwaysRequeue, NeverRequeue},
        },
        BrokerAction, ConsumerGroup, ConsumerPreStartHook, ConsumerTransientErrorHook, Delivery,
        ErrorType, Handler, HandlerError, Incoming, MessageHandler, MessageProcessing,
        ProcessingOutcome, ShouldRequeue, TelemetryMiddleware,
    },
    publishers::MessageEnvelope,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn if_a_message_level_pre_start_hook_is_provided_the_group_level_one_is_ignored() {
    #[derive(Clone, Default)]
    struct CountingHook {
        counter: Arc<Mutex<u64>>,
    }

    #[async_trait::async_trait]
    impl ConsumerPreStartHook for CountingHook {
        async fn run(
            &self,
            _channel: &Channel,
            _queue_name: &str,
            _args: FieldTable,
        ) -> Result<(), Error> {
            *self.counter.lock().await += 1;
            Ok(())
        }
    }

    // Arrange

    let group_hook = CountingHook::default();
    let handler_hook = CountingHook::default();
    let consumer_group_builder = ConsumerGroup::builder(get_connection_factory(), Context)
        .with_pre_start_hook(group_hook.clone())
        // With custom hook
        .message_handler(
            MessageHandler::builder(Uuid::new_v4().to_string())
                .with_pre_start_hook(handler_hook.clone())
                .handler(OkHandler),
        )
        // Without custom hook
        .message_handler(MessageHandler::builder(Uuid::new_v4().to_string()).handler(OkHandler));

    // Act
    let _ = consumer_group_builder.build().await.unwrap();

    // Assert
    assert_eq!(1, *group_hook.counter.lock().await);
    assert_eq!(1, *handler_hook.counter.lock().await);
}

#[tokio::test]
async fn if_multiple_pre_hooks_are_set_they_are_all_called() {
    #[derive(Clone, Default)]
    struct CountingHook {
        counter: Arc<Mutex<u64>>,
    }

    #[async_trait::async_trait]
    impl ConsumerPreStartHook for CountingHook {
        async fn run(
            &self,
            _channel: &Channel,
            _queue_name: &str,
            _args: FieldTable,
        ) -> Result<(), Error> {
            *self.counter.lock().await += 1;
            Ok(())
        }
    }

    // Arrange

    let handler_hook = CountingHook::default();
    let consumer_group_builder = ConsumerGroup::builder(get_connection_factory(), Context)
        // With custom hook
        .message_handler(
            MessageHandler::builder(Uuid::new_v4().to_string())
                .with_pre_start_hook(handler_hook.clone())
                .with_pre_start_hook(handler_hook.clone())
                .handler(OkHandler),
        )
        // Without custom hook
        .message_handler(MessageHandler::builder(Uuid::new_v4().to_string()).handler(OkHandler));

    // Act
    let _ = consumer_group_builder.build().await.unwrap();

    // Assert
    assert_eq!(2, *handler_hook.counter.lock().await);
}
#[tokio::test]
async fn if_a_pre_start_hook_returns_an_error_the_consumer_group_fails_to_build() {
    #[derive(Clone, Default)]
    struct FailingHook;

    #[async_trait::async_trait]
    impl ConsumerPreStartHook for FailingHook {
        async fn run(
            &self,
            _channel: &Channel,
            _queue_name: &str,
            _args: FieldTable,
        ) -> Result<(), Error> {
            Err(anyhow::anyhow!("Kaboom."))
        }
    }

    // Arrange
    let consumer_group_builder = ConsumerGroup::builder(get_connection_factory(), Context)
        .with_pre_start_hook(FailingHook)
        .message_handler(MessageHandler::builder(Uuid::new_v4().to_string()).handler(OkHandler));

    // Act
    let outcome = consumer_group_builder.build().await;

    // Assert
    assert!(outcome.is_err());
}

#[tokio::test]
async fn if_a_message_level_transient_error_hook_is_provided_the_group_level_one_is_ignored() {
    #[derive(Clone, Default)]
    struct CountingHook {
        counter: Arc<Mutex<u64>>,
    }

    #[async_trait::async_trait]
    impl ConsumerTransientErrorHook for CountingHook {
        async fn on_transient_error(&self, _delivery: &Delivery) -> ShouldRequeue {
            {
                *self.counter.lock().await += 1;
            }
            ShouldRequeue::DeadLetterOrDiscard
        }
    }

    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let publisher = get_publisher().await;

    let group_hook = CountingHook::default();
    let handler_hook = CountingHook::default();
    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .transient_error_hook(group_hook.clone())
        .exit_after(1)
        // With custom hook
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_pre_start_hook(DeclareDurableQueue)
                .transient_error_hook(handler_hook.clone())
                .handler(TransientErrorHandler),
        )
        .build()
        .await
        .unwrap();

    // Act
    let handle = tokio::spawn(consumer_group.run_until_sigterm());

    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: first_queue_name,
            properties: <_>::default(),
        })
        .await
        .unwrap();

    handle.await.unwrap().unwrap();

    // Assert
    assert_eq!(0, *group_hook.counter.lock().await);
    assert_eq!(1, *handler_hook.counter.lock().await);
}

#[derive(Clone, Default)]
struct RequeueFlagMiddleware {
    last_message_was_requeued: Arc<Mutex<bool>>,
}

#[async_trait::async_trait]
impl<Context: Sync + Send + 'static> TelemetryMiddleware<Context, ()> for RequeueFlagMiddleware {
    async fn handle<'a>(
        &'a self,
        incoming: &'a Incoming<Context>,
        next: MessageProcessing<'a, Context, ()>,
    ) -> ProcessingOutcome<()> {
        let outcome = next.run(incoming).await;
        {
            *self.last_message_was_requeued.lock().await = outcome.was_requeued();
        }
        outcome
    }
}

#[tokio::test]
async fn if_a_message_failed_with_a_fatal_error_was_requeued_returns_false() {
    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let flag_middleware = RequeueFlagMiddleware::default();
    let publisher = get_publisher().await;

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .exit_after(1)
        // With custom hook
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_telemetry_middleware(flag_middleware.clone())
                .with_pre_start_hook(DeclareDurableQueue)
                .handler(FatalErrorHandler),
        )
        .build()
        .await
        .unwrap();

    // Act
    let handle = tokio::spawn(consumer_group.run_until_sigterm());

    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: first_queue_name,
            properties: <_>::default(),
        })
        .await
        .unwrap();

    handle.await.unwrap().unwrap();

    // Assert
    assert!(!*flag_middleware.last_message_was_requeued.lock().await);
}

#[tokio::test]
async fn was_requeued_returns_true_for_requeued_transient_errors() {
    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let flag_middleware = RequeueFlagMiddleware::default();
    let publisher = get_publisher().await;

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_telemetry_middleware(flag_middleware.clone())
                // We ALWAYS requeue
                .transient_error_hook(AlwaysRequeue)
                .with_pre_start_hook(DeclareDurableQueue)
                .handler(TransientErrorHandler),
        )
        .build()
        .await
        .unwrap();

    // Act
    let handle = tokio::spawn(consumer_group.run_until_sigterm());

    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: first_queue_name,
            properties: <_>::default(),
        })
        .await
        .unwrap();

    handle.await.unwrap().unwrap();

    // Assert
    assert!(*flag_middleware.last_message_was_requeued.lock().await);
}

#[tokio::test]
async fn was_requeued_returns_false_for_non_requeued_transient_errors() {
    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let flag_middleware = RequeueFlagMiddleware::default();
    let publisher = get_publisher().await;

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_telemetry_middleware(flag_middleware.clone())
                // We NEVER requeue
                .transient_error_hook(NeverRequeue)
                .with_pre_start_hook(DeclareDurableQueue)
                .handler(TransientErrorHandler),
        )
        .build()
        .await
        .unwrap();

    // Act
    let handle = tokio::spawn(consumer_group.run_until_sigterm());

    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: first_queue_name,
            properties: <_>::default(),
        })
        .await
        .unwrap();

    handle.await.unwrap().unwrap();

    // Assert
    assert!(!*flag_middleware.last_message_was_requeued.lock().await);
}

#[tokio::test]
async fn was_requeued_returns_false_for_successes() {
    // Arrange
    let first_queue_name = Uuid::new_v4().to_string();
    let flag_middleware = RequeueFlagMiddleware::default();
    let publisher = get_publisher().await;

    let consumer_group = ConsumerGroup::builder(get_connection_factory(), Context)
        .exit_after(1)
        .message_handler(
            MessageHandler::builder(&first_queue_name)
                .with_telemetry_middleware(flag_middleware.clone())
                .with_pre_start_hook(DeclareDurableQueue)
                .handler(OkHandler),
        )
        .build()
        .await
        .unwrap();

    // Act
    let handle = tokio::spawn(consumer_group.run_until_sigterm());

    publisher
        .publish(MessageEnvelope {
            payload: "Not relevant".into(),
            exchange_name: "".into(),
            routing_key: first_queue_name,
            properties: <_>::default(),
        })
        .await
        .unwrap();

    handle.await.unwrap().unwrap();

    // Assert
    assert!(!*flag_middleware.last_message_was_requeued.lock().await);
}

#[derive(Clone)]
struct Context;

struct TransientErrorHandler;
#[async_trait]
impl Handler for TransientErrorHandler {
    type Context = Context;
    type Error = ();
    async fn handle(
        &self,
        _incoming: &Incoming<Context>,
    ) -> Result<BrokerAction, HandlerError<()>> {
        Err(HandlerError {
            inner_error: (),
            error_type: ErrorType::Transient,
        })
    }
}
struct FatalErrorHandler;
#[async_trait]
impl Handler for FatalErrorHandler {
    type Context = Context;
    type Error = ();
    async fn handle(
        &self,
        _incoming: &Incoming<Context>,
    ) -> Result<BrokerAction, HandlerError<()>> {
        Err(HandlerError {
            inner_error: (),
            error_type: ErrorType::Fatal,
        })
    }
}
struct OkHandler;
#[async_trait]
impl Handler for OkHandler {
    type Context = Context;
    type Error = ();
    async fn handle(
        &self,
        _incoming: &Incoming<Context>,
    ) -> Result<BrokerAction, HandlerError<()>> {
        Ok(BrokerAction::Ack)
    }
}
