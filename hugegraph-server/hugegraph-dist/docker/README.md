# Deploy Hugegraph server with docker

> Note:
> 
> 1. The docker image of hugegraph is a convenience release, not official distribution artifacts from ASF. You can find more details from [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html#dockerhub).
> 
> 2. Recommand to use `release tag`(like `1.2.0`) for the stable version. Use `latest` tag to experience the newest functions in development.

## 1. Deploy

We can use docker to quickly start an inner HugeGraph server with RocksDB in background.

1. Using docker run

    Use `docker run -itd --name=graph -p 8080:8080 hugegraph/hugegraph` to start hugegraph server.

2. Using docker compose

    Certainly we can only deploy server without other instance. Additionally, if we want to manage other HugeGraph-related instances with `server` in a single file, we can deploy HugeGraph-related instances via `docker-compose up -d`.  The `docker-compose.yaml` is as below:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph
        ports:
          - 8080:8080
    ```

## 2. Create Sample Graph on Server Startup

If you want to **pre-load** some (test) data or graphs in container(by default), you can set the env `PRELOAD=ture`

If you want to customize the pre-loaded data, please mount the the groovy scripts (not necessary).

1. Using docker run

    Use `docker run -itd --name=graph -p 8080:8080 -e PRELOAD=true -v /path/to/yourScript:/hugegraph/scripts/example.groovy hugegraph/hugegraph`
    to start hugegraph server.

2. Using docker compose 

    We can also use `docker-compose up -d` to quickly start. The `docker-compose.yaml` is below. [example.groovy](https://github.com/apache/incubator-hugegraph/blob/master/hugegraph-dist/src/assembly/static/scripts/example.groovy) is a pre-defined script. If needed, we can mount a new `example.groovy` to preload different data:

    ```yaml
    version: '3'
    services:
      graph:
        image: hugegraph/hugegraph
        environment:
          - PRELOAD=true
        volumes:
          - /path/to/yourscript:/hugegraph/scripts/example.groovy
        ports:
          - 8080:8080
    ```

3. Using start-hugegraph.sh

    If you deploy HugeGraph server without docker, you can also pass arguments using `-p`, like this: `bin/start-hugegraph.sh -p true`.

## 3. Enable Authentication

1. Using docker run

    Use `docker run -itd --name=graph -p 8080:8080 -e AUTH=true -e PASSWORD=123456 hugegraph/hugegraph` to enable the authentication and set the password with `-e AUTH=true -e PASSWORD=123456`.

2. Using docker compose

    Similarly, we can set the envionment variables in the docker-compose.yaml:

    ```yaml
    version: '3'
    services:
      server:
        image: hugegraph/hugegraph
        container_name: graph
        ports:
          - 8080:8080
        environment:
          - AUTH=true
          - PASSWORD=123456
    ```