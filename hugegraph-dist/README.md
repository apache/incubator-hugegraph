# Deploy Hugegraph server with docker compose(WIP)

## 1. Deploy

We can use `docker-compose up -d` to quickly start an inner HugeGraph server with RocksDB in background.

The docker-compose.yaml is below:

```yaml
version: '3'
services:
  graph:
    image: hugegraph/hugegraph
    ports:
      - 18080:8080
```

## 2. Create Sample Graph on Server Startup

If you want to pre-load some data or graphs in container, you can set the env `PRELOAD=ture`

If you want to customize the pre-loaded data, please mount the the groovy scripts

```yaml
version: '3'
services:
  graph:
    image: hugegraph/hugegraph
    environment:
      - PRELOAD=true
    volumes:
      - /path/to/yourscript:/hugegraph/scripts/example-preload.groovy
    ports:
      - 18080:8080
```