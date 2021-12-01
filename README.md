# hugegraph-commons

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![codecov](https://codecov.io/gh/hugegraph/hugegraph-common/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph-common)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.baidu.hugegraph/hugegraph-common/badge.svg)](https://mvnrepository.com/artifact/com.baidu.hugegraph/hugegraph-common)

hugegraph-commons is a common module for [HugeGraph](https://github.com/hugegraph/hugegraph) and its peripheral components.
hugegraph-commons encapsulates locks, configurations, events, iterators, rest and some 
numeric or collection util classes to simplify the development of HugeGraph and 
its components.

## Components

- Lock: atomic lock, key lock, lock group and lock manger
- Config: register and load config option with security check
- Event: listening and notification, do something asynchronously
- Iterator: some iterators with extra functions, map, filter, extend etc.
- Rest: RESTful client implemented on Jersey, POST, PUT, GET and DELETE
- Util: performance analyzer, version checker, numeric and Collection utils, log and exception utils etc.
- Rpc: rpc component for inner module communication, currently it's based on [Sofa-RPC](https://github.com/sofastack/sofa-rpc)

## Licence
The same as HugeGraph, hugegraph-commons is also licensed under Apache 2.0 License.
