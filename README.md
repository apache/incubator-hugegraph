<h1 align="center">
    <img width="720" alt="hugegraph-logo" src="https://github.com/apache/incubator-hugegraph/assets/38098239/e02ffaed-4562-486b-ba8f-e68d02bb0ea6" style="zoom:100%;" />
</h1>

<h3 align="center">A graph database that supports more than 10 billion data, high performance and scalability</h3>

<div align="center">

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![HugeGraph-CI](https://github.com/apache/incubator-hugegraph/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/incubator-hugegraph/actions/workflows/ci.yml)
[![License checker](https://github.com/apache/incubator-hugegraph/actions/workflows/licence-checker.yml/badge.svg)](https://github.com/apache/incubator-hugegraph/actions/workflows/licence-checker.yml)
[![GitHub Releases Downloads](https://img.shields.io/github/downloads/apache/hugegraph/total.svg)](https://github.com/apache/hugegraph/releases)

</div>

## What is Apache HugeGraph?

[HugeGraph](https://hugegraph.apache.org/) is a fast-speed and highly-scalable [graph database](https://en.wikipedia.org/wiki/Graph_database). 
Billions of vertices and edges can be easily stored into and queried from HugeGraph due to its excellent OLTP ability. 
As compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/) framework, various complicated graph queries can be 
achieved through [Gremlin](https://tinkerpop.apache.org/gremlin.html)(a powerful graph traversal language).


## Features

- Compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/), support [Gremlin](https://tinkerpop.apache.org/gremlin.html) & [Cypher](https://en.wikipedia.org/wiki/Cypher) language
- Schema Metadata Management, including VertexLabel, EdgeLabel, PropertyKey and IndexLabel
- Multi-type Indexes, supporting exact query, range query and complex conditions combination query
- Plug-in Backend Store Driver Framework, support `RocksDB`, `Cassandra`, `HBase`, `ScyllaDB`, and `MySQL/Postgre` now and easy to add another backend store driver if needed
- Integration with `Flink/Spark/HDFS`, and friendly to connect other big data platforms


## Quick Start

### 1. Docker Way (Convenient for Test)

We can use `docker run -itd --name=graph -p 8080:8080 hugegraph/hugegraph` to quickly start an inner 
HugeGraph server with `RocksDB` (in backgrounds) for **test/dev**.
You can visit [doc page](https://hugegraph.apache.org/docs/quickstart/hugegraph-server/#3-deploy) or the [README](hugegraph-server/hugegraph-dist/docker/READEME.md) for more details.

> Note:
>  
> 1. The docker image of hugegraph is a convenience release, but not **official distribution** artifacts. You can find more details from [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html#dockerhub).
> 
> 2. Recommend to use `release tag`(like `1.2.0`) for the stable version. Use `latest` tag to experience the newest functions in development.

### 2. Download Way

Visit [Download Page](https://hugegraph.apache.org/docs/download/download/) and refer the [doc](https://hugegraph.apache.org/docs/quickstart/hugegraph-server/#32-download-the-binary-tar-tarball) 
to download the latest release package and start the server.

### 3. Source Building Way

Visit [Source Building Page](https://hugegraph.apache.org/docs/quickstart/hugegraph-server/#33-source-code-compilation) and follow the 
steps to build the source code and start the server.

The project [doc page](https://hugegraph.apache.org/docs/) contains more information on HugeGraph
and provides detailed documentation for users. (Structure / Usage / API / Configs...)

And here are links of other **HugeGraph** component/repositories:
1. [hugegraph-toolchain](https://github.com/apache/incubator-hugegraph-toolchain) (graph tools **[loader](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-loader)/[dashboard](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-hubble)/[tool](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-tools)/[client](https://github.com/apache/incubator-hugegraph-toolchain/tree/master/hugegraph-client)**)
2. [hugegraph-computer](https://github.com/apache/incubator-hugegraph-computer) (integrated **graph computing** system)
3. [hugegraph-commons](https://github.com/apache/incubator-hugegraph-commons) (**common & rpc** libs)
4. [hugegraph-website](https://github.com/apache/incubator-hugegraph-doc) (**doc & website** code)



## License

HugeGraph is licensed under [Apache 2.0 License](LICENSE).


## Contributing

- Welcome to contribute to HugeGraph, please see [`How to Contribute`](CONTRIBUTING.md) & [Guidelines](https://hugegraph.apache.org/docs/contribution-guidelines/) for more information.  
- Note: It's recommended to use [GitHub Desktop](https://desktop.github.com/) to greatly simplify the PR and commit process.  
- Thank you to all the people who already contributed to HugeGraph!

[![contributors graph](https://contrib.rocks/image?repo=apache/hugegraph)](https://github.com/apache/incubator-hugegraph/graphs/contributors)


## Thanks

HugeGraph relies on the [TinkerPop](http://tinkerpop.apache.org) framework, we refer to the storage structure of Titan and the schema definition of DataStax. 
Thanks to TinkerPop, thanks to Titan, thanks to DataStax. Thanks to all other organizations or authors who contributed to the project.

You are welcome to contribute to HugeGraph, 
and we are looking forward to working with you to build an excellent open-source community.


## Contact Us

- [GitHub Issues](https://github.com/apache/incubator-hugegraph/issues): Feedback on usage issues and functional requirements (quick response)
 - Feedback Email: [dev@hugegraph.apache.org](mailto:dev@hugegraph.apache.org) ([subscriber](https://hugegraph.apache.org/docs/contribution-guidelines/subscribe/) only)
 - WeChat public account: Apache HugeGraph, welcome to scan this QR code to follow us.

 <img src="https://github.com/apache/incubator-hugegraph-doc/blob/master/assets/images/wechat.png?raw=true" alt="QR png" width="300"/>

