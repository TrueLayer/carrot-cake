# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2]

- Made `RabbitMqTlsSettings::ca_certificate_chain_pem` from `String` to `Option<String>` public

## [0.1.1]

- Changed `RabbitMqTlsSettings::ca_certificate_chain_pem` from `String` to `Option<String>`
- Changed `RabbitMqSettings::password` from `String` to `redact::Secret<String>`

## [0.1.0]

- Initial release
