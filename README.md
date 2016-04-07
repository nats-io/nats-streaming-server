# Project STAN

STAN is an extremely performant, lightweight reliable streaming platform built on [NATS](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT) 
[![Build Status](https://travis-ci.com/nats-io/stan-server.svg?token=UGjrGa8sFWGQcHSJeAvp&branch=master)](http://travis-ci.com/nats-io/stan-server)
[![Coverage Status](https://coveralls.io/repos/nats-io/stan-server/badge.svg?branch=master)](https://coveralls.io/r/nats-io/stan-server?branch=master)

[Project Design Document](https://docs.google.com/document/d/1keDwK35YQnOXXKKy2HVV2oOnvEUPFyypT-Tplh8F89c/edit)

STAN provides the following high-level feature set.
- Log based.
- At-Least-Once Delivery model, giving reliable message delivery.
- Rate matched on a per subscription basis.
- Replay/Restart
- Last Value Semantics
