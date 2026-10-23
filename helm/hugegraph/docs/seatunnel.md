# HugeGraph 与 SeaTunnel 联合部署

SeaTunnel 默认关闭。给 HugeGraph Helm 命令增加 `--set seatunnel.enabled=true`，即可同时启动一个持续接收作业的单机引擎；增加 `--set seatunnel.mode=cluster` 则使用分离的 master/worker 集群。

| 配置 | 作用 |
|---|---|
| HugeGraph `mode: easy/strict` | 访问入口和 NetworkPolicy 默认值 |
| `values-single.yaml` / `values-cluster.yaml` | HugeGraph 自身拓扑 |
| `seatunnel.mode: standalone` | 一个默认混合角色节点，同时承担 master 和 worker |
| `seatunnel.mode: cluster` | 独立 master、worker Deployment，默认各两个副本 |

两套拓扑独立选择。SeaTunnel 源码由 [HugeGraph SeaTunnel fork](https://github.com/hugegraph/seatunnel/tree/dev/deploy/kubernetes/seatunnel) 维护，复用官方启动脚本、ConfigMap 挂载和 Headless Service 发现方式；HugeGraph 只引入固定版本的打包依赖，别名为 `seatunnel`，不维护第二套模板，也不需要 Operator 或 CRD。

单机使用官方脚本的默认混合角色，不传 `-r`。SeaTunnel 3.0.0 的 Java 入口只接受显式的 `master` 或 `worker`，传入 `-r master_and_worker` 会启动失败；本次启动实测已发现并修正这一点。

## 启动与切换

以下命令从 HugeGraph 仓库根目录执行，选择一种安装方式：

```bash
# 小型 HugeGraph + 单机 SeaTunnel
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-standalone.yaml \
  --wait --timeout 15m

# HugeGraph 集群 + 分离的 SeaTunnel master/worker
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  -f helm/hugegraph/examples/values-seatunnel-cluster.yaml \
  --wait --timeout 15m
```

HugeGraph 的存储、调度和凭据要求见 [Chart README](../README.md)。默认镜像为 `apache/seatunnel:3.0.0`。release 为 `hugegraph` 时：

| 资源 | 用途 |
|---|---|
| `hugegraph-seatunnel-master` Deployment | 单机混合节点或专用 master |
| `hugegraph-seatunnel-worker` Deployment | 仅集群模式创建 |
| `hugegraph-seatunnel` Headless Service | TCP 5801 成员发现 |
| `hugegraph-seatunnel-master` Service | TCP 8080 内部 REST API |
| `hugegraph-seatunnel-configs` ConfigMap | 引擎、Hazelcast、JVM 和日志配置 |

名称可由 `seatunnel.fullnameOverride` 修改；DNS 后缀由 `seatunnel.clusterDomain` 指定，默认为 `cluster.local`。两种 HugeGraph 访问模式都不会自动公开 SeaTunnel API。

切换前先停止作业或创建 savepoint，保留当前完整 values 和凭据，再升级：

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph -f my-values.yaml \
  --set seatunnel.enabled=true --set seatunnel.mode=cluster \
  --set seatunnel.master.replicas=2 --set seatunnel.worker.replicas=2 \
  --wait --timeout 15m
```

切回单机设置 `seatunnel.mode=standalone`、`seatunnel.master.replicas=1`。单机使用 Recreate 策略避免两个混合节点重叠；模式切换会重启 master，作业不会自动迁移或重新提交。设为 `seatunnel.enabled=false` 会移除本 release 管理的 SeaTunnel，不撤回已写入的图数据，也不删除外部 Secret 或存储。

## 连接器镜像与凭据

先确认所选镜像包含与引擎版本一致的 `connector-hugegraph` 及其依赖。已有合适镜像可以直接使用；Chart 不根据镜像标签推断连接器清单。如需固定连接器组合，可使用 [示例 Dockerfile](../examples/seatunnel/Dockerfile)：

```bash
docker build -t registry.example.com/data/seatunnel-hugegraph:3.0.0 \
  helm/hugegraph/examples/seatunnel
docker push registry.example.com/data/seatunnel-hugegraph:3.0.0
```

它在构建时执行官方 `install-plugin.sh 3.0.0`，安装 HugeGraph、FakeSource、Console。实际数据源所需的其他连接器和驱动也应在构建时加入；Helm 不下载连接器或自动提交作业。

准备 `hugegraph-seatunnel-credentials` Secret，包含作业账号的 `username`、`password` 两个键。将以下配置合并到安装使用的 values 文件，镜像已有连接器时保留自己的镜像引用即可：

```yaml
seatunnel:
  image:
    registry: registry.example.com/data/seatunnel-hugegraph
    tag: "3.0.0"
    digest: "" # 可选，设置后优先于 tag
    pullSecret: ""
  env:
    - name: HG_USERNAME
      valueFrom:
        secretKeyRef:
          name: hugegraph-seatunnel-credentials
          key: username
    - name: HG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: hugegraph-seatunnel-credentials
          key: password
```

这些变量同时进入 master 和 worker，作业文件通过 `${HG_USERNAME}`、`${HG_PASSWORD}` 读取凭据。Chart 不创建 HugeGraph 账号，也不复制管理员密码；账号需有目标图的读写及 schema 权限。更新 Secret 后需重启相关 Pod，刷新环境变量。

`global.imageRegistry` 同样作用于 SeaTunnel；显式指定完整仓库地址优先。SeaTunnel 保留上游的 `image.registry`、`image.pullSecret` 键，不继承 HugeGraph 顶层的 `imagePullSecrets`。

## 场景一：直接导入 Graph A

示例链路为 `FakeSource → graph_a`，导入固定的十条数据：`person_01/21` 到 `person_10/30`。随后通过 `HugeGraph Source → HugeGraph Sink` 复制到 `graph_b`。示例仅使用 `seatunnel_demo_person` 顶点标签，主键为 `name`，属性为 `name`、`age`。

先在 `DEFAULT` 图空间准备两个空图 `graph_a`、`graph_b`。可通过 Hubble 创建，或使用有建图权限的账号调用 API。以下 curl 在本机执行，需先由凭据管理器向本机环境提供 `HG_USERNAME`、`HG_PASSWORD`；Pod 的环境变量不会自动传到本机。

```bash
# 在另一个终端保持端口转发
kubectl -n hugegraph port-forward svc/hugegraph-server 8082:8080
```

```bash
export HG_URL=http://127.0.0.1:8082
for demo_graph in graph_a graph_b; do
  curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
    -X POST "$HG_URL/graphspaces/DEFAULT/graphs/$demo_graph" -d '{}'
done
```

已有图不重复执行建图命令。按实际 release、Server 端口、图空间调整示例连接信息，再提交 [hugegraph-sink.conf](../examples/seatunnel/hugegraph-sink.conf)：

```bash
kubectl -n hugegraph exec -i deploy/hugegraph-seatunnel-master -- \
  sh -c 'cat > /tmp/hugegraph-sink.conf' \
  < helm/hugegraph/examples/seatunnel/hugegraph-sink.conf
kubectl -n hugegraph exec deploy/hugegraph-seatunnel-master -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster --config /tmp/hugegraph-sink.conf
```

`--master cluster` 表示客户端提交到已启动的引擎，单机混合节点也使用这个参数。CLI 未开启 `--async`，等待作业结束。检查作业成功，并用 Gremlin 核对 Graph A 的顶点数量：

```bash
curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  "$HG_URL/gremlin" -d '{"gremlin":"graph.traversal().V().hasLabel(\"seatunnel_demo_person\").count()","aliases":{"graph":"DEFAULT-graph_a"}}'
```

空图首次导入的 `result.data` 应为 `[10]`。[hugegraph-source.conf](../examples/seatunnel/hugegraph-source.conf) 可按相同方式提交，将 Graph A 的内容输出到 worker 日志；单机模式查看 master 日志。

## 场景二：Graph A 复制到 Graph B

[hugegraph-copy.conf](../examples/seatunnel/hugegraph-copy.conf) 使用 HugeGraph 作为 source 和 sink，读取 `graph_a`，写入 `graph_b`。目标标签和属性不存在时由 sink 创建，两个图本身仍需预先存在。

```bash
kubectl -n hugegraph exec -i deploy/hugegraph-seatunnel-master -- \
  sh -c 'cat > /tmp/hugegraph-copy.conf' \
  < helm/hugegraph/examples/seatunnel/hugegraph-copy.conf
kubectl -n hugegraph exec deploy/hugegraph-seatunnel-master -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster --config /tmp/hugegraph-copy.conf
```

作业成功后，比较两图数量及按 `name` 排序后的逐条内容：

```bash
curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  "$HG_URL/gremlin" -d '{"gremlin":"def scan = { x -> x.traversal().V().hasLabel(\"seatunnel_demo_person\").order().by(\"name\").project(\"name\",\"age\").by(\"name\").by(\"age\").toList() }; def aa=scan(a); def bb=scan(b); [count_a:aa.size(),count_b:bb.size(),same:aa==bb,rows:bb]","aliases":{"a":"DEFAULT-graph_a","b":"DEFAULT-graph_b"}}'
```

期望两边均为十条，`same=true`，目标数据包含 `person_01/21` 到 `person_10/30`。这里复制的是一个顶点标签，不是自动克隆全图 schema、多标签和全部边。复制边时需补充对应映射，并先写入端点顶点；其他 ID 策略按连接器规则配置。

更多配置见 [HugeGraph source](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/source/HugeGraph.md)、[sink](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/sink/HugeGraph.md) 和 [HugeGraph 连接器指南](https://hugegraph.apache.org/cn/docs/quickstart/toolchain/import/hugegraph-seatunnel-connector/)。Pod Ready、`helm --wait`、作业成功和图数据一致是不同层面的检查。

## 存储与网络

`seatunnel.engineConfig` 合并原生 `seatunnel.engine` 配置。默认 checkpoint 使用临时本地目录；跨 Pod 恢复需配置共享持久化存储及相应客户端依赖、凭据和网络，例如：

```yaml
seatunnel:
  engineConfig:
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.com:8020
          namespace: /seatunnel/checkpoints
```

额外挂载使用 `seatunnel.extraVolumes`、`extraVolumeMounts`。完整文件覆盖使用 `seatunnel.configMap.data`；单机 JVM 文件是 `jvm_options`，集群是 `jvm_master_options`、`jvm_worker_options`。原始 `data["seatunnel.yaml"]` 会覆盖引擎配置合并结果。

使用外部 ConfigMap 时设置 `configMap.create=false` 和 `existingConfigMapName`，必须提供全部十个文件：`hazelcast.yaml`、`hazelcast-client.yaml`、`hazelcast-master.yaml`、`hazelcast-worker.yaml`、`jvm_options`、`jvm_client_options`、`jvm_master_options`、`jvm_worker_options`、`log4j2.properties`、`seatunnel.yaml`。生成配置变化会触发滚动更新，外部 ConfigMap 因为 `subPath` 挂载需要手动重启 Pod。

开启 NetworkPolicy 时自动允许 SeaTunnel 成员间 TCP 5801、HTTP 端口、DNS，以及访问本 release 的 HugeGraph Server API。外部数据源、checkpoint 服务和 REST 调用方通过 `networkPolicy.seatunnel.extraEgress/extraIngress` 补充原生规则；实际隔离依赖 CNI 支持。使用外部 SeaTunnel 时保持 `seatunnel.enabled=false`，按实际来源配置 `networkPolicy.server.extraIngress`。

本集成保留官方 Helm 的 Deployment 方式；[官方 Kubernetes 指南](https://seatunnel.apache.org/zh-CN/docs/3.0.0/getting-started/kubernetes/) 推荐生产环境使用 StatefulSet。副本数增加不等于 checkpoint 持久化，生产前需验证存储、调度和故障恢复。

## 维护与验证记录

SeaTunnel 模板只在 fork 的 `deploy/kubernetes/seatunnel` 维护。两个仓库并列放置时，在 HugeGraph 根目录执行：

```bash
helm lint ../seatunnel/deploy/kubernetes/seatunnel
helm dependency update helm/hugegraph
helm lint helm/hugegraph --set seatunnel.enabled=true
```

版本变化时更新父 Chart 的依赖版本，移除旧归档，提交新归档及 `Chart.lock`。原生 `file://../../../seatunnel/deploy/kubernetes/seatunnel` 依赖负责打包；最终安装使用随 HugeGraph 发布的归档，不需要相邻源码仓库或联网下载 Chart。

2026-10-23 本地验证记录（台北时间）：

| 检查 | 结果与范围 |
|---|---|
| Python Helm 回归 | 23 项通过 |
| Helm 单元测试 | 309 项通过 |
| HugeGraph 推送前检查 | `mvn editorconfig:format` 通过；`mvn clean compile -Dmaven.javadoc.skip=true` 全部 38 模块通过 |
| Kubernetes 真实 API | 安装及模式切换已执行；业务 Pod Ready 为 0，尚未证明集群内引擎可用 |
| 本机引擎启动 | 官方 SeaTunnel 3.0.0 二进制、Java 17；`/overview` 返回版本 3.0.0、一个 worker |
| 直接导入 graph_a | 固定十条 FakeSource 数据；作业 `1166014497995882497` 为 `FINISHED`，读取与提交均为 10 |
| graph_a → graph_b | 作业 `1166014497996079105` 为 `FINISHED`；使用文档中的 CLI 与环境变量方式再次复制，作业 `1166014645144649729` 读取 10、写入 10、失败 0 |
| 图数据核对 | 隔离的 HugeGraph 1.8.0/RocksDB；Gremlin 返回 `count_a=10`、`count_b=10`、`same=true`，逐条比对 `person_01` 至 `person_10` 的 name/age 完全一致 |

以上业务记录来自本机二进制引擎，不等同于 Kubernetes 内完整业务验证；隔离 Kubernetes 中的引擎启动和同样两条业务路径正在补测。此次数据样例只有一个顶点标签，没有验证多标签、边、HStore 或生产故障恢复。
