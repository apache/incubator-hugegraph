# HugeGraph Server

HugeGraph Server consists of two layers of functionality: the graph engine layer, and the storage layer.

- Graph Engine Layer:
  - REST Server: Provides a RESTful API for querying graph/schema information, supports the [Gremlin](https://tinkerpop.apache.org/gremlin.html) and [Cypher](https://en.wikipedia.org/wiki/Cypher) query languages, and offers APIs for service monitoring and operations.
  - Graph Engine: Supports both OLTP and OLAP graph computation types, with OLTP implementing the [Apache TinkerPop3](https://tinkerpop.apache.org) framework.
  - Backend Interface: Implements the storage of graph data to the backend.

- Storage Layer:
  - Storage Backend: Supports multiple built-in storage backends (RocksDB/MySQL/HBase/...) and allows users to extend custom backends without modifying the existing source code.
