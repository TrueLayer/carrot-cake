# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4](https://github.com/TrueLayer/carrot-cake/compare/v0.1.3...v0.1.4) - 2024-05-02

### Other
- Manual pub ([#18](https://github.com/TrueLayer/carrot-cake/pull/18))

- Initial release
- Changed `RabbitMqTlsSettings::ca_certificate_chain_pem` from `String` to `Option<String>`
- Changed `RabbitMqSettings::password` from `String` to `redact::Secret<String>`
