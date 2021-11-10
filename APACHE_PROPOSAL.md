# [New Podling Proposal of HugeGraph](https://cwiki.apache.org/confluence/display/INCUBATOR/New+Podling+Proposal)

## Abstract

HugeGraph will be a graph database with high performance and scalability.

## Proposal

HugeGraph is to provide an large-scale graph database, which achieves availability and balance between performance and cost in the scenario of 100+ billion data,  and has complete HTAP capabilities in an internal system.

### Background

Graph databases are good at dealing with relational analysis, but relational databases are generally not good at due to slow join performance, especially in the case of large-scale data with multiple dimensions and deep association relationships. HugeGraph was born in Baidu, the dominant search engine company in China, used to solve the large-scale graph analysis requirements of anti-fraud and protection from black market attacks.

### Rationale

In the industry, graph databases can generally handle 1-billion-scale graphs, there is a lack of systems for processing 100-billion-scale graphs, and some graph databases may only support online query (OLTP), and some graph platforms only support graph computing (OLAP). HugeGraph supports both online query and graph computing in the scenario of 100+ billion data.
