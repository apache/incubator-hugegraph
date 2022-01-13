<div align="center">
    <img width="720" alt="hugegraph-logo" src="https://user-images.githubusercontent.com/17706099/149281100-c296db08-2861-4174-a31f-e2a92ebeeb72.png" style="zoom:100%;" />
</div>

<p align="center">
    A graph database that supports more than 10+ billion data, high performance and scalability
</p>
<hr/>

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://github.com/hugegraph/hugegraph/actions/workflows/ci.yml/badge.svg)](https://github.com/hugegraph/hugegraph/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/hugegraph/hugegraph/branch/master/graph/badge.svg)](https://codecov.io/gh/hugegraph/hugegraph)
[![Github Releases Downloads](https://img.shields.io/github/downloads/hugegraph/hugegraph/total.svg)](https://github.com/hugegraph/hugegraph/releases)

HugeGraph is a fast-speed and highly-scalable [graph database](https://en.wikipedia.org/wiki/Graph_database). Billions of vertices and edges can be easily stored into and queried from HugeGraph due to its excellent OLTP ability. As compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/) framework, various complicated graph queries can be accomplished through [Gremlin](https://tinkerpop.apache.org/gremlin.html)(a powerful graph traversal language).

## Features

- Compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/), supporting [Gremlin](https://tinkerpop.apache.org/gremlin.html)
- Schema Metadata Management, including VertexLabel, EdgeLabel, PropertyKey and IndexLabel
- Multi-type Indexes, supporting exact query, range query and complex conditions combination query
- Plug-in Backend Store Driver Framework, supporting RocksDB, Cassandra, ScyllaDB, HBase and MySQL now and easy to add other backend store driver if needed
- Integration with Hadoop/Spark

## Getting Started

The project [homepage](https://hugegraph.github.io/hugegraph-doc/) contains more information on HugeGraph and provides links to **documentation**, getting-started guides and release downloads.

## Contributing

Welcome to contribute to HugeGraph, please see [`How to Contribute`](CONTRIBUTING.md) for more information.

## License

HugeGraph is licensed under Apache 2.0 License.

## Thanks

HugeGraph relies on the [TinkerPop](http://tinkerpop.apache.org) framework, we refer to the storage structure of Titan and the schema definition of DataStax. 
Thanks to TinkerPop, thanks to Titan, thanks to DataStax. Thanks to all other organizations or authors who contributed to the project.

You are welcome to contribute to HugeGraph, and we are looking forward to working with you to build an excellent open source community.
