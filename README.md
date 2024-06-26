# carrot-cake
## An a-peeling pub/sub framework

test fake release

[![Crates.io](https://img.shields.io/crates/v/carrot-cake.svg)](https://crates.io/crates/carrot-cake)
[![Docs.rs](https://docs.rs/carrot-cake/badge.svg)](https://docs.rs/carrot-cake)
[![CI](https://github.com/truelayer/carrot-cake/workflows/CI/badge.svg)](https://github.com/truelayer/carrot-cake/actions)

carrot-cake is TrueLayer's take on an opinionated and flexible RabbitMQ PubSub framework.

## Features:

* Multiple consumers per vhost
* Concurrent handling
* Consumer Middlewares
* Consumer pre-start hooks to set up queues and exchanges
* Publishers backed by a connection pool
* Publisher Middlewares

## Installation

Add `carrot-cake` to your dependencies

```toml
[dependencies]
# ...
carrot-cake = "0.1.0"
```

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

See [CONTRIBUTING.md](CONTRIBUTING.md).
