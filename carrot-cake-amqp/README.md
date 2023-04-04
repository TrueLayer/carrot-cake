# core-banking amqp integration library
[![CircleCI](https://circleci.com/gh/TrueLayer/rust-amqp/tree/main.svg?style=svg&circle-token=823c17ca3ec00f0ffd261ff3d3ff5f21005ff999)](https://circleci.com/gh/TrueLayer/rust-amqp/tree/main)

Library to provide an abstraction on top of lapin for publishing messages to and consuming messages
from a RabbitMQ message exchange.

## Prerequisites

- Rust (see [here](https://www.rust-lang.org/tools/install) for instructions)
- Docker (see [here](https://docs.docker.com/install/) for instructions)

## How to build

```bash
cargo build --all --all-targets
```

## How to test

The tests need RabbitMq to run.

- Start RabbitMq Docker container:

```bash
./scripts/init_rabbitmq.sh
```

- Run tests

```bash
cargo test
```

## How to use

Check the live documentation at [https://rust.t7r.dev](https://rust.t7r.dev/carrot_cake_amqp/index.html).

You can also generate the documentation locally with

```bash
cargo doc --no-deps --all-features --open
```

Or refer to our example application under [`examples/publish.rs`](https://github.com/TrueLayer/rusty-bunny/tree/main/src/amqp/examples/publish.rs).
You can run the example with

```bash
cargo run --example publish
```
