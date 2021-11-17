# [New Podling Proposal of HugeGraph](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal)

## Abstract

HugeGraph is a graph database with high performance and scalability.

## Proposal

HugeGraph is to provide an large-scale graph database, in the scenario of 100+ billion data which achieves availability and balance between performance and cost, and has complete HTAP capabilities in an internal system.

We believe that bringing HugeGraph into Apache Software Foundation could advance the development of a stronger and more diverse open source community.

### Background

Graph databases are good at dealing with relational analysis, but relational databases are generally at a disadvantage due to slow join performance, especially in the case of large-scale graph with multiple dimensions and deep association relationships. HugeGraph was born at Baidu, the dominant search engine company in China, used to solve the large-scale graph analysis requirements of anti-fraud and protection from black market attacks.

### Rationale

In the industry, graph databases can generally handle 1-billion-scale graphs, there is a lack of systems for processing 100-billion-scale graphs, and some graph databases may only support online query (OLTP), some graph platforms only support graph computing (OLAP). HugeGraph supports both online query and graph computing in the scenario of 100+ billion data.

### Initial Goals

None

### Current Status

#### Meritocracy

HugeGraph was incubated at Baidu in 2016 and open sourced on [GitHub](https://github.com/hugegraph/hugegraph) in 2018. The project(include sub-projects) now has 30+ contributors from many companies, some of them become committers, and has hundreds of known users around the world. We have set up the PMC Team and Committer Team, contributions are welcomed and highly valued, new contributors are guided and reviewed by existed PMC members. When an active contributor has submitted enough good patches, PMC will start a vote to promote him/her to become a member of Committer Team or PMC Team.

#### Community

Baidu has been building a community around users and developers for the last 3 years, now we make use of GitHub as code hosting and community communication. The most of core developers are from Baidu, there are 10+ contributors from non-Baidu companies like 360, Iflytek and Huya. We hope to grow the base of contributors by inviting all those who offer contributions through The Apache Way. 

#### Core Developers

The core developers are already experienced open source developers, they have operated the HugeGraph Community for 3 years, and they are contributors of Linux kernel, OpenStack, Ceph, RocksDB, Apache TinkerPop, Apache Hadoop and Apache Groovy.

#### Alignment

HugeGraph is compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/) framework, various complicated graph queries can be accomplished through [Gremlin](https://tinkerpop.apache.org/gremlin.html), a powerful graph traversal language. The TinkerPop ecosystem based on Java is relatively mature in terms of graph databases, we use Java language which is develop efficient and stable to develop upper-level components like graph engine, graph computing, graph api and graph tools; and we manage storage through JNI which is able to freely manage memory and execute efficiently.

### Known Risks

#### Project Name

We have checked that the name is [suitable](https://github.com/hugegraph/hugegraph/issues/1646) and the project has legal permission to continuing using its current name. There is no one else found using this name through Google search.

#### Relationship with Titan/Janus Graph

In the early stage of the project, we referred to the storage structure of Titan/Janus Graph, some folks thought that HugeGraph was forked from Titan/Janus. In fact, HugeGraph is not based on its code, it's completely self-developed, and addressed many new challenges. Of course, we are still inspired by and thank for Titan/Janus.

### Orphaned products

Due to only about 10 committers, the project has a relatively small risk of becoming an orphan. However, the committers have been operating the HugeGraph Community for 3 years in the spirit of open source, and continue to develop new contributors to participate.

#### Inexperience with Open Source:

HugeGraph has been open sourced on GitHub for 3 years, during which committers submitted code and documents in full compliance with open source specifications and requirements.

#### Length of Incubation:

Expect to enter incubation in 4 months and graduate in about 2 years.

#### Homogenous Developers:

The current list of developers from several different companies plus many independent volunteers, but the most of committers are from Baidu. The developers are geographically concentrated in China now. They are experienced with working in a distributed environment in other open source project, e.g. OpenStack.

#### Reliance on Salaried Developers:

Most of the developers are paid by their employer to contribute to this project, but given some volunteer developers and the committers' sense of ownership for the code, the project would continue without issue if no salaried developers contributed to the project.

#### Relationships with Other Apache Products:

HugeGraph follows the Apache TinkerPop specification, uses Apache Commons, Apache HttpClient, and Apache HttpCore to implement the basic functions. Users can choose Apache Cassandra or Apache HBase as one of the storage backends of HugeGraph.

#### A Excessive Fascination with the Apache Brand:

Although we expect that the Apache brand may help attract more contributors, our interest in starting this project is based on the factors mentioned in the fundamentals section. We are interested in joining ASF to increase our connections in the open source world. Based on extensive collaboration, it is possible to build a community of developers and committers that live longer than the founder.

### Documentation

HugeGraph documentation is provided on https://hugegraph.github.io/hugegraph-doc/ in Simplified Chinese, the complete English version of the documentation is being prepared.

### Initial Source

This project consists of 2 core sub-projects and 8 complementary sub-projects, all of them are hosted by [GitHub  hugegraph organization](https://github.com/orgs/hugegraph/repositories) since 2018, the codes are already under Apache License Version 2.0. The git address of sub-project repositories are as follows:

1. The graph database repository `hugegraph`, core sub-project, including graph server, graph engine and graph storage: https://github.com/hugegraph/hugegraph
2. The graph computing repository `hugegraph-computer`, core sub-project, including graph computing and graph algorithm s: https://github.com/hugegraph/hugegraph-computer
3. `hugegraph-common`: https://github.com/hugegraph/hugegraph-common
4. `hugegraph-client`: https://github.com/hugegraph/hugegraph-client
5. `hugegraph-doc`: https://github.com/hugegraph/hugegraph-doc
6. `hugegraph-loader`: https://github.com/hugegraph/hugegraph-loader
7. `hugegraph-tools`: https://github.com/hugegraph/hugegraph-tools
8. `hugegraph-rpc`: https://github.com/hugegraph/hugegraph-rpc
9. `hugegraph-hubble`: https://github.com/hugegraph/hugegraph-hubble
10. `hugegraph-test`: https://github.com/hugegraph/hugegraph-test

### Source and Intellectual Property Submission Plan

The codes are currently under Apache License Version 2.0 and have been verified to there is no intellectual property or license issues before being released to open source by Baidu in 2018. Baidu will provide SGA and all committers will sign ICLA after HugeGraph is accepted into the Incubator.

#### External Dependencies:

As all dependencies are managed using Apache Maven, none of the external libraries need to be packaged in a source distribution. All dependencies have Apache compatible licenses except MySQL (GPL-2.0), we will remove MySQL dependencies in future.

HugeGraph has the following external [dependencies](https://github.com/hugegraph/hugegraph/issues/1632):

- Apache License

  - HttpClient
  - HttpCore

- Apache License 2.0

  - Annotations for Metrics
  - ansj_seg
  - Apache Cassandra
  - Apache Commons
  - Apache Groovy
  - Apache HttpClient
  - Apache HttpCore
  - Apache Ivy
  - Apache Log4j
  - Apache Thrift
  - Apache TinkerPop
  - Apache Yetus
  - Bean Validation API
  - Caffeine cache
  - CLI
  - Sofa
  - ConcurrentLinkedHashMap
  - Data Mapper for Jackson
  - DataStax Java Driver for Apache Cassandra
  - Disruptor Framework
  - error-prone annotations
  - exp4j
  - fastutil
  - Findbugs
  - Google Android Annotations Library
  - Gson
  - Guava
  - Hibernate Validator Engine
  - HPPC Collections
  - htrace-core4
  - IKAnalyzer
  - io.grpc
  - J2ObjC Annotations
  - Jackson
  - Java Agent for Memory Measurements
  - Java Concurrency Tools Core Library
  - JavaPoet
  - javatuples
  - jcseg-core
  - jffi
  - JJWT
  - jnr
  - Joda
  - jraft
  - JSON.simple
  - JVM Integration for Metrics
  - Lucene
  - LZ4 and xxHash
  - Metrics
  - mmseg4j-core
  - Netty
  - Ning-compress-LZF
  - nlp-lang
  - Objenesis
  - OHC core
  - OpenTracing
  - perfmark
  - picocli
  - proto-google-common-protos
  - sigar
  - snappy-java
  - stream-lib
  - swagger
  - Thrift Server implementation backed by LMAX Disruptor
  - jieba for java
  - Java Native Access
  - HanLP
  - SnakeYAML

- Apache License 2.0 + GPLv2 License

  - RocksDB JNI

- BSD License

  - ANTLR 3
  - ASM
  - Chinese to Pinyin
  - Hamcrest
  - JLine
  - jcabi
  - PostgreSQL JDBC Driver - JDBC 4.2
  - Protocol Buffers
  - StringTemplate

- CDDL + GPL License

  - Grizzly
  - Jersey

- CDDL + GPLv2 License

  - glassfish
  - Java Servlet API
  - javax.ws.rs-api 
  - jersey-inject-hk2
  - JSR 353 (JSON Processing) Default Provider

- Commercial License + AGPL License

  - TrueLicense

- Eclipse Distribution License

  - Eclipse Collections
  - JUnit

- ISC/BSD License

  - jBCrypt

- LGPL License

  - JBoss Logging 3

- LGPL + GPL License License

  - jnr-posix

- LGPL 2.1 + MPL 1.1 + Apache License 2.0

  - Javassist

- MIT License

  - Animal Sniffer Annotations
  - Mockito
  - SLF4J
  - Jedis
  - high-scale-lib
  - jnr-x86asm

- GPL + MIT License

  - Checker Qual

- GPL License

  - MySQL Connector/J
  - word

#### Cryptography

None

### Required Resources

#### Mailing lists:

- hugegraph-dev: [dev@hugegraph.incubator.apache.org](mailto:dev@hugegraph.incubator.apache.org) for development and users discussions.
- hugegraph-private: [private@hugegraph.incubator.apache.org](mailto:private@hugegraph.incubator.apache.org) for PPMC discussions.
- hugegraph-commits: [commits@hugegraph.incubator.apache.org](mailto:commits@hugegraph.incubator.apache.org) for commits / [pull requests](https://github.com/hugegraph/hugegraph/pulls) and other notifications like code review comments.

#### [Subversion Directory:](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal#subversion-directory)

None

#### [Git Repositories:](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal#git-repositories)

1. `hugegraph`: https://github.com/hugegraph/hugegraph.git
2. `hugegraph-computer`: https://github.com/hugegraph/hugegraph-computer.git
3. `hugegraph-common`: https://github.com/hugegraph/hugegraph-common.git
4. `hugegraph-client`: https://github.com/hugegraph/hugegraph-client.git
5. `hugegraph-doc`: https://github.com/hugegraph/hugegraph-doc.git
6. `hugegraph-loader`: https://github.com/hugegraph/hugegraph-loader.git
7. `hugegraph-tools`: https://github.com/hugegraph/hugegraph-tools.git
8. `hugegraph-rpc`: https://github.com/hugegraph/hugegraph-rpc.git
9. `hugegraph-hubble`: https://github.com/hugegraph/hugegraph-hubble.git
10. `hugegraph-test`: https://github.com/hugegraph/hugegraph-test.git

#### Issue Tracking:

We choose JIRA HugeGraph (HUGEGRAPH), and the community would like to continue using [GitHub Issues](https://github.com/hugegraph/hugegraph/issues).

#### Other Resources:

- The community has already choosed [GitHub actions](https://github.com/hugegraph/hugegraph/actions) as continuous integration tools.
- The community has already used [codecov](https://github.com/marketplace/codecov) to check code coverage.
- The community has already used [mvn repository](https://mvnrepository.com/search?q=hugegraph) as binary package release platform.

### Initial Committers

- 李章梅, Jermy Li (javaloveme at gmail dot com)
- 李凝瑞, linary (liningrui at vip dot qq dot com)
- 张义, zhoney (zhangyi89817 at 126 dot com)
- 金子威, imbajin (0x00 at imbajin.com)
- 章炎, zyxxoo (1318247699 at qq dot com)
- 刘楠科, liunanke (liunanke at baidu dot com)
- 赵聪, coderzc (zc1217zc at 126 dot com)
- 郭守敬, ShouJing (1075185785 at qq dot com)
- 李小彪, lxb1111 (13703287619 at 163 dot com)
- 黄海平, hhpcn (954872405 at qq dot com)
- 江小叶, kidleaf-jiang (kidleaf at 163 dot com)

### Sponsors

#### Champion:

- 姜宁, Willem Ning Jiang (ningjiang at apache dot org)

#### Nominated Mentors:

- 潘娟
- 代立冬
- 黄向东
- 李钰

#### Sponsoring Entity:

We are expecting the Apache Incubator could sponsor this project.
