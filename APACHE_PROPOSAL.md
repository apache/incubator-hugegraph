# [New Podling Proposal of HugeGraph](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal)

## Abstract

HugeGraph will be a graph database with high performance and scalability.

## Proposal

HugeGraph is to provide an large-scale graph database, which achieves availability and balance between performance and cost in the scenario of 100+ billion data,  and has complete HTAP capabilities in an internal system.

### Background

Graph databases are good at dealing with relational analysis, but relational databases are generally not good at due to slow join performance, especially in the case of large-scale data with multiple dimensions and deep association relationships. HugeGraph was born in Baidu, the dominant search engine company in China, used to solve the large-scale graph analysis requirements of anti-fraud and protection from black market attacks.

### Rationale

In the industry, graph databases can generally handle 1-billion-scale graphs, there is a lack of systems for processing 100-billion-scale graphs, and some graph databases may only support online query (OLTP), some graph platforms only support graph computing (OLAP). HugeGraph supports both online query and graph computing in the scenario of 100+ billion data.

### Initial Goals

None

### Current Status

#### Meritocracy

HugeGraph was incubated at Baidu in 2016 and open sourced on [GitHub](https://github.com/hugegraph/hugegraph) in 2018. The project now has 30+ contributors from many companies, some of them become committers, and has hundreds of known users around the world. We have set up the PMC Team and Committer Team, contributions are welcomed and highly valued, new contributors are guided and reviewed by existed PMC members. When an active contributor has submitted enough good patches, PMC will start a vote to promote him/her to become a member of Committer Team or PMC Team.

#### Community

Baidu has been building a community around users and developers for the last three years, now we make use of GitHub as code hosting and community communication. There are 10+ contributors from non-Baidu companies like 360, Kedaxunfei, Huyazhibo. We hope to grow the base of contributors by inviting all those who offer contributions through The Apache Way. 

#### Core Developers

The core developers are already very experienced open source developers, they have managed the hugegraph community for three years, and they are contributors of Linux kernel, OpenStack, Ceph, RocksDB, Apache TinkerPop, Apache Hadoop and Apache Groovy.

#### Alignment

HugeGraph is compliance to [Apache TinkerPop 3](https://tinkerpop.apache.org/) framework, various complicated graph queries can be accomplished through [Gremlin](https://tinkerpop.apache.org/gremlin.html), a powerful graph traversal language. The TinkerPop ecosystem based on Java is relatively mature in terms of graph databases, we use Java language which is develop efficient and stable to develop upper-level components like graph engine, graph computing, graph api and graph tools; and we manage storage through JNI which is able to freely manage memory and execute efficiently.

### Known Risks

#### Project Name

We have checked that the name is [suitable](https://github.com/hugegraph/hugegraph/issues/1646) and the project has legal permission to continuing using its current name. There is no one else found using this name through Google search.

#### Relationship with Titan/Janus Graph

In the early stage of the project, we referred to the storage structure of Titan/Janus Graph, some folks thought that HugeGraph was forked from Titan/Janus. In fact, HugeGraph is not based on its code, completely self-developed, and addressed many new challenges. Of course, we are still inspired by and thank for Titan/Janus.
