# Deploy Hugegraph server with docker

> Note:
> 
> 1. The docker image of hugegraph is a convenience release, not official distribution artifacts from ASF. You can find more details from [ASF Release Distribution Policy](https://infra.apache.org/release-distribution.html#dockerhub).
> 
> 2. Recommend to use `release tag`(like `1.2.0`) for the stable version. Use `latest` tag to experience the newest functions in development.

## 1. Deploy

We can use docker to quickly start an inner HugeGraph server with RocksDB in the background.

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

If you want to **preload** some (test) data or graphs in container(by default), you can set the env `PRELOAD=ture`

If you want to customize the preloaded data, please mount the groovy scripts (not necessary).

1. Using docker run

    Use `docker run -itd --name=graph -p 8080:8080 -e PRELOAD=true -v /path/to/yourScript:/hugegraph/scripts/example.groovy hugegraph/hugegraph`
    to start hugegraph server.

2. Using docker compose 

    We can also use `docker-compose up -d` to quickly start. The `docker-compose.yaml` is below. [example.groovy](https://github.com/apache/incubator-hugegraph/blob/master/hugegraph-server/hugegraph-dist/src/assembly/static/scripts/example.groovy) is a pre-defined script. If needed, we can mount a new `example.groovy` to preload different data:

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

    Similarly, we can set the environment variables in the docker-compose.yaml:

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
## 4. Running Open-Telemetry-Collector

> CAUTION:
> 
> The `docker-compose-trace.yaml` utilizes `Grafana` and `Grafana-Tempo`, both of them are licensed under [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html), you should be aware of and use them with caution. Currently, we mainly provide this template for everyone to **test**
>
1. Start Open-Telemetry-Collector

    ```bash
    cd hugegraph-server/hugegraph-dist/docker/example
    docker-compose -f docker-compose-trace.yaml -p hugegraph-trace up -d
    ```
   
2. Active Open-Telemetry-Agent

    ```bash
    ./start-hugegraph.sh -y true
    ```
   
3. Stop Open-Telemetry-Collector

    ```bash
    cd hugegraph-server/hugegraph-dist/docker/example
    docker-compose -f docker-compose-trace.yaml -p hugegraph-trace stop
    ```
   
4. References

   - [What is OpenTelemetry](https://opentelemetry.io/docs/what-is-opentelemetry/)

   - [Tempo in Grafana](https://grafana.com/docs/tempo/latest/getting-started/tempo-in-grafana/)
