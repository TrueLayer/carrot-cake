use anyhow::Error;
use carrot_cake::consumers::hooks::pre_start::DurableExchangeBinder;
use carrot_cake::consumers::hooks::transient_error::{AlwaysRequeue, NeverRequeue};
use carrot_cake::consumers::{
    ConsumerGroup, ErrorType, HandlerError, Incoming, MessageHandler, MessageProcessing, Next,
    ProcessingMiddleware, ProcessingOutcome, TelemetryMiddleware,
};
use carrot_cake::{configuration::RabbitMqSettings, rabbit_mq::ConnectionFactory};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First of all we build the configuration for our connection factory.
    // We are using the out-of-the-box parameters for the default RabbitMq Docker image.
    let settings = RabbitMqSettings::default();
    let connection_factory = ConnectionFactory::new_from_config(&settings)?;

    // In message handlers you will often need to use resources with a significant initialisation
    // time - e.g. a HTTP client, a database connection, etc.
    // Instead of creating a new instance of these expensive resources every single time you handle
    // a message, you can put those resources in the _context_.
    //
    // The context is created once, before the consumer group is built, and each message handler
    // gets a shared reference (&) to the context together with the incoming message.
    // You can therefore retrieve the HTTP client or the database connection pool from the
    // context without having to initialise them from scratch.
    //
    // In this sample we are using a dummy `Context` type holding an integer.
    let context = Context { seed: 42 };

    // We want all message handlers to consume from the same exchange, but using a different
    // routing key. We declare a closure, parametrised by the `routing_key`,
    // that we can then call for each handler as appropriate.
    let bind_queue = |routing_key: &str| DurableExchangeBinder {
        exchange_name: "test-exchange".into(),
        routing_key: routing_key.into(),
    };

    // If you are building a message consumer, you'll be working with `ConsumerGroup`.
    // A consumer group is a set of message handlers sharing the same context.
    //
    // `truelayer-pubsub` provides a fluent API to build consumers by adding configuration
    // in a step-by-step fashion.
    // Certain types of configuration values can only be added at the group level (e.g. connection
    // factory, context, queue name prefix), others can be set both at the group and message
    // handler level (e.g. lifecycle hooks).
    //
    // We'll go through each method in this example and explain what each statement does.
    //
    // To build a `ConsumerGroup`, you call the `ConsumerGroup::builder` method.
    // `ConsumerGroup::builder` asks upfront for two pieces of required information:
    // the connection factory, which will be used to talk to the RabbitMq broker, and the context,
    // shared by all message handlers in the group.
    let consumer_group = ConsumerGroup::builder(connection_factory, context)
        // Using `queue_name_prefix` you can add a prefix to all the queues associated with
        // message handlers in this group.
        // Using `test` we will have `test_odd-messages-queue` and `test_even-messages-queue` in
        // this example as queue names.
        // The prefix is optional - you can choose not to specify any for your consumer group.
        .queue_name_prefix("test")
        // A transient error hook allows you to customise the behaviour of the message handlers
        // when the processing of an incoming message fails with an error classified as transient
        // - e.g. retryable.
        // We are using `NeverRequeue` - nack the message with the broker and do not requeue.
        // Check `rust-amqp-retry` for more advanced retry logic for transient failures.
        .transient_error_hook(NeverRequeue)
        // You can add middleware to inject logic before and after the handler logic.
        // Middlewares are executed in the order they are registered: the first registered
        // middleware executes first on the way in and last on the way out.
        //
        // In our case, for the first handler:
        //
        // DummyTelemetryMiddleware ->
        //  DummyProcessingMiddlewareA ->
        //    DummyProcessingMiddlewareB ->
        //      handler ->
        //    DummyProcessingMiddlewareB ->
        //  DummyProcessingMiddlewareA ->
        //  Ack/Nack with the broker ->
        // DummyTelemetryMiddleware
        //
        // For the second handler:
        //
        // DummyTelemetryMiddleware ->
        //  DummyProcessingMiddlewareA ->
        //    handler ->
        //  DummyProcessingMiddlewareA ->
        //  Ack/Nack with the broker ->
        // DummyTelemetryMiddleware
        //
        // Check the `truelayer-pubsub-observability` crate for telemetry middlewares.
        .with_processing_middleware(DummyProcessingMiddlewareA)
        .with_telemetry_middleware(DummyTelemetryMiddleware)
        // Once we are done with the group-level configuration we can start adding message handlers.
        .message_handler(
            // `MessageHandler::builder` is the entrypoint to start configuring a message handler.
            // `MessageHandler::builder` asks you for the only piece of mandatory information: the
            // name of the queue your consumer will be pulling messages from.
            MessageHandler::builder("odd-messages-queue")
                // Pre-start hooks are executed _before_ we start consuming messages from the queue.
                // They are used to execute setup logic for resources against the message broker
                // - e.g. create exchanges, bind queues, etc.
                //
                // In this case we use a pre-start hook to declare the exchange and bind our queue
                // to that exchange using `odd` as routing key.
                .with_pre_start_hook(bind_queue("odd"))
                // You can register middleware at the message handler level as well!
                // Handler-specific middlewares are executed after the group-level middleware chain
                // of the corresponding type (processing/telemetry).
                .with_processing_middleware(DummyProcessingMiddlewareB)
                // Passing in the handler function finalises the `MessageHandler` construction.
                .handler(odd_handler),
        )
        .message_handler(
            MessageHandler::builder("even-messages-queue")
                // We use a pre-start hook to declare the exchange and bind our queue to that exchange
                // using `even` as routing key.
                .with_pre_start_hook(bind_queue("even"))
                // Although you can register hooks at the group level, you can override them
                // at the message handler level if you need to.
                //
                // This message handler will always requeue messages whose processing failed with
                // a transient error, even if the group-level setting is `NeverRequeue`.
                .transient_error_hook(AlwaysRequeue)
                .handler(even_handler),
        )
        // Once you have added all your message handlers to the group, you can finalise it calling
        // `build`.
        //
        // When you await `build`, we establish a connection with the message broker and run all
        // the pre-start hooks.
        // After you call `build` the group does NOT start consuming messages (yet)!
        .build()
        .await?;

    // You can call `run_until_stopped` to start consuming messages from the queues you bound.
    // `run_until_stopped` returns control to the caller only if:
    // - one of the message handlers crashes (e.g. disconnection);
    // - the application is stopped (as the name of the function implies).
    //
    // You usually want to invoke this method in your `main` function after you have performed all the setup
    // you wanted to do.
    consumer_group.run_until_stopped().await?;

    Ok(())
}

pub struct Context {
    seed: u64,
}

/// A dummy message handler - processing succeeds if the message payload has an odd number of bytes,
/// it fails otherwise.
pub async fn odd_handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError<Error>> {
    // `data` is the message payload.
    let n_bytes = incoming.message.data.len();
    if n_bytes % 2 == 1 {
        Ok(())
    } else {
        Err(HandlerError {
            inner_error: anyhow::anyhow!(
                "Expected an odd number of bytes, got {} bytes with seed {}",
                n_bytes,
                // We are accessing the (boring) shared context here!
                incoming.context.seed
            ),
            // We are marking the error as fatal - never to retried.
            error_type: ErrorType::Fatal,
        })
    }
}

/// A dummy message handler - processing succeeds if the message payload has an even number of bytes,
/// it fails otherwise.
pub async fn even_handler(incoming: Incoming<'_, Context>) -> Result<(), HandlerError<Error>> {
    let n_bytes = incoming.message.data.len();
    if n_bytes % 2 == 0 {
        Ok(())
    } else {
        Err(HandlerError {
            inner_error: anyhow::anyhow!(
                "Expected an even number of bytes, got {} bytes with seed {}",
                n_bytes,
                incoming.context.seed
            ),
            error_type: ErrorType::Fatal,
        })
    }
}

// Three dummy middlewares:
// - the first two are processing middlewares, they influence the processing outcome;
// - the second is a telemetry middleware, it logs out the outcome.
//
// Check out the `truelayer-pubsub-observability` crate for useful telemetry middlewares.

pub struct DummyProcessingMiddlewareA;

#[async_trait::async_trait]
impl ProcessingMiddleware<Context, Error> for DummyProcessingMiddlewareA {
    async fn handle<'a>(
        &'a self,
        incoming: Incoming<'a, Context>,
        next: Next<'a, Context, Error>,
    ) -> Result<(), HandlerError<Error>> {
        let outcome = next.run(incoming).await;
        // Change the outcome - nothing is transient here!
        outcome.map_err(|mut e| {
            e.error_type = ErrorType::Fatal;
            e
        })
    }
}

pub struct DummyProcessingMiddlewareB;

#[async_trait::async_trait]
impl ProcessingMiddleware<Context, Error> for DummyProcessingMiddlewareB {
    async fn handle<'a>(
        &'a self,
        incoming: Incoming<'a, Context>,
        next: Next<'a, Context, Error>,
    ) -> Result<(), HandlerError<Error>> {
        let outcome = next.run(incoming).await;
        // Change the outcome - nothing is fatal here!
        outcome.map_err(|mut e| {
            e.error_type = ErrorType::Transient;
            e
        })
    }
}

pub struct DummyTelemetryMiddleware;

#[async_trait::async_trait]
impl TelemetryMiddleware<Context, Error> for DummyTelemetryMiddleware {
    async fn handle<'a>(
        &'a self,
        incoming: Incoming<'a, Context>,
        next: MessageProcessing<'a, Context, Error>,
    ) -> ProcessingOutcome<Error> {
        let outcome = next.run(incoming).await;
        match outcome.result() {
            Ok(_) => {
                println!("All good!");
            }
            Err(e) => {
                println!("Something went wrong: {e:?}");
            }
        }
        outcome
    }
}
