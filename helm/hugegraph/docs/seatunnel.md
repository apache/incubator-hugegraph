# HugeGraph 与 SeaTunnel 联合部署

本 Chart 直接复用 [SeaTunnel 3.0.0 官方 hybrid Kubernetes 方案](https://github.com/apache/seatunnel/blob/v3.0.0/docs/zh/getting-started/kubernetes/hybrid-cluster-mode.md)，在 HugeGraph Helm 中适配资源名称、DNS、镜像、配置和副本数，不修改 SeaTunnel Helm，也不引入子 Chart、Operator 或 CRD。

SeaTunnel 默认关闭。启用后，一个常驻 hybrid 节点同时承担调度和执行，持续接收任务；集群模式只是把同一个 StatefulSet 扩为多个 hybrid 节点。它与执行完即退出的 `local-mode` Pod/Job 不同。

| 配置 | 含义 |
|---|---|
| `values-single.yaml` | HugeGraph 1 PD + 1 Store + 1 Server，使用 HStore |
| `values-cluster.yaml` | HugeGraph 自身的多副本拓扑 |
| `seatunnel.mode: standalone` | 一个常驻 hybrid 节点 |
| `seatunnel.mode: cluster`、`replicas: 2` | 两个 hybrid 节点，副本数至少为 2 |

HugeGraph 与 SeaTunnel 的拓扑独立选择；HugeGraph 的 `mode: easy/strict` 仍只控制其访问入口和 NetworkPolicy 默认值。

## 启动、访问前端与切换

在 HugeGraph 仓库根目录执行，以下两种安装方式选择一种：

```bash
# HugeGraph 1 PD + 1 Store + 1 Server，加一个常驻 SeaTunnel 节点
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-standalone.yaml \
  --wait --timeout 15m

# 同样的 HugeGraph 拓扑，加两个 SeaTunnel hybrid 节点
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-cluster.yaml \
  --wait --timeout 15m
```

默认使用 `apache/seatunnel:3.0.0`。两种模式都开启引擎自带的 HTTP 服务和任务可视化前端，无需另部署 Web 服务或数据库：

```bash
kubectl -n hugegraph port-forward svc/hugegraph-seatunnel 8081:8080
```

打开 <http://127.0.0.1:8081/#/overview>，查看集群、作业、DAG 和日志。作业仍通过 SeaTunnel CLI 或 REST API 提交；Helm 只启动引擎，不自动建图或导入数据。

release 为 `hugegraph` 时，资源为 `hugegraph-seatunnel` StatefulSet、同名 HTTP Service、`hugegraph-seatunnel-headless` 发现 Service，以及 `hugegraph-seatunnel-configs` ConfigMap。名称遵循 HugeGraph 的 `fullnameOverride`；成员通过 Headless Service 的 DNS 发现，不访问 Kubernetes API，因此不需要发现用的 RBAC。

切换前先停止全部作业，保留安装使用的完整 values，再升级：

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph -f my-values.yaml \
  --set seatunnel.enabled=true --set seatunnel.mode=cluster \
  --set seatunnel.replicas=2 --wait --timeout 15m
```

切回单机设置 `seatunnel.mode=standalone`。两种模式使用同一个 StatefulSet，但副本和引擎配置会变化；不承诺无中断切换，也不会自动恢复或重新提交作业。设置 `seatunnel.enabled=false` 移除本 release 的 SeaTunnel，已经写入 HugeGraph 的数据保留。

## 镜像、凭据与原生配置

优先复用官方镜像或已有镜像，需确认其包含版本匹配的 HugeGraph 连接器及依赖。镜像通过 `seatunnel.image.repository/tag/pullPolicy` 选择，继承 HugeGraph 的 `global.imageRegistry` 和 `imagePullSecrets`；Helm 不在启动时下载插件。

凭据使用原生 Kubernetes Secret 引用。例如，本地验证可直接引用默认 Helm 管理员 Secret，无需复制密码：

```yaml
seatunnel:
  env:
    - name: HG_USERNAME
      value: admin
    - name: HG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: hugegraph-admin
          key: password
```

账号需有作业涉及的图、数据和 Schema 权限。实际作业可改用专用账号。所有 hybrid 节点接收同样的环境变量；更新 Secret 后重启相关 Pod，刷新环境变量。作业文件通过 `${HG_USERNAME}`、`${HG_PASSWORD}` 读取凭据。

`seatunnel.javaOpts` 调整引擎 JVM，`resources` 调整容器资源。高级配置保留 SeaTunnel 原生文件格式，通过 `seatunnel.config` 按文件名完整覆盖，不逐项增加 Helm 开关，例如：

```yaml
seatunnel:
  javaOpts: "-Xms512m -Xmx1g"
  config:
    jvm_client_options: |
      -Xms128m
      -Xmx256m
```

默认生成 `hazelcast.yaml`、`hazelcast-client.yaml`、`seatunnel.yaml` 和 JVM 文件，其余配置沿用镜像。覆盖发现或引擎文件时保留 TCP 5801、HTTP 8080 和 `enable-http: true`，才能继续使用现有 Service、探针及前端。默认 checkpoint 使用临时本地目录；本集成不提供持久化作业恢复保证。

strict 模式允许 SeaTunnel 成员通信、DNS，以及访问同 release 的 HugeGraph Server。远端图 A、其他数据源及调用方，通过 `networkPolicy.seatunnel.extraEgress/extraIngress` 补充原生规则；实际隔离依赖 CNI 支持。HugeGraph 的凭据、存储和访问方式见 [Chart README](../README.md)。

## 简单导入：十条数据写入 Graph A

[hugegraph-sink.conf](../examples/seatunnel/hugegraph-sink.conf) 使用 `FakeSource → HugeGraph`，写入 `seatunnel_demo_person` 标签的十条固定数据。先创建 `DEFAULT/graph_a`；本机 curl 所用的 `HG_USERNAME`、`HG_PASSWORD` 需由凭据管理器提供，Pod 环境变量不会传到本机。

```bash
# 在另一个终端保持转发
kubectl -n hugegraph port-forward svc/hugegraph-server 8082:8080
```

```bash
export HG_URL=http://127.0.0.1:8082
curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  -X POST "$HG_URL/graphspaces/DEFAULT/graphs/graph_a" -d '{}'

kubectl -n hugegraph exec -i sts/hugegraph-seatunnel -- \
  sh -c 'cat > /tmp/hugegraph-sink.conf' \
  < helm/hugegraph/examples/seatunnel/hugegraph-sink.conf
kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster --config /tmp/hugegraph-sink.conf

curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  "$HG_URL/gremlin" -d '{"gremlin":"graph.traversal().V().hasLabel(\"seatunnel_demo_person\").count()","aliases":{"graph":"DEFAULT-graph_a"}}'
```

已有图不重复建图。首次空图导入后，作业应为 `FINISHED`，读取、写入各十条，Gremlin 的 `result.data` 为 `[10]`。`--master cluster` 表示提交给常驻引擎，单节点也使用该参数。

## 新环境复制：Schema、顶点和边

这里使用已有图 A 和另一个环境中的空图 B，不接续上面的十条数据示例。复制顺序是 **Schema → 顶点 → 边**。SeaTunnel Sink 的自动建 Schema 只能根据映射和行类型创建所需结构，不能替代源图完整 Schema、索引及 ID 的复制。

先通过 HugeGraph 原生 API 创建空的 `graph_b`，再执行 [copy-schema.py](../examples/seatunnel/clone/copy-schema.py)。脚本在目标图的 RESTORING 状态下保留源 Schema ID，复制属性键、顶点标签、边标签和索引；不传输图数据。源、目标应使用兼容的 HugeGraph 版本，账号需有相应管理权限，目标 Schema 必须为空。

```bash
# 两个 URL 均包含图空间及图名；对应账号密码由本机环境提供
export HG_SOURCE_GRAPH_URL=https://source.example.com/graphspaces/DEFAULT/graphs/graph_a
export HG_TARGET_GRAPH_URL=https://target.example.com/graphspaces/DEFAULT/graphs/graph_b
python3 helm/hugegraph/examples/seatunnel/clone/copy-schema.py \
  --source "$HG_SOURCE_GRAPH_URL" --target "$HG_TARGET_GRAPH_URL"
```

脚本读取 `HG_SOURCE_USERNAME/HG_SOURCE_PASSWORD` 和 `HG_TARGET_USERNAME/HG_TARGET_PASSWORD`。随后在 SeaTunnel Pod 中配置相同的两组凭据，以及 `HG_SOURCE_HOST/PORT/PROTOCOL`、`HG_TARGET_HOST/PORT/PROTOCOL`；密码继续用 Secret 注入。源、目标地址必须能从 SeaTunnel Pod 访问，不能填写仅在本机可达的端口转发地址。

四个示例作业读取 `graph_a`、写入 `graph_b`，先复制两个顶点标签，再复制两类边：

```bash
for clone_job in person company works_at knows; do
  kubectl -n hugegraph exec -i sts/hugegraph-seatunnel -- \
    sh -c "cat > /tmp/$clone_job.conf" \
    < "helm/hugegraph/examples/seatunnel/clone/$clone_job.conf"
  kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
    /opt/seatunnel/bin/seatunnel.sh --master cluster --config "/tmp/$clone_job.conf" \
    || break
done
```

作业使用 `ERROR_WHEN_SCHEMA_NOT_EXIST`，复用已经复制的 Schema；边依赖先前写入的端点顶点。这组映射覆盖 [person](../examples/seatunnel/clone/person.conf)、[company](../examples/seatunnel/clone/company.conf)、[works_at](../examples/seatunnel/clone/works_at.conf) 和 [knows](../examples/seatunnel/clone/knows.conf)。复制其他图时需按实际标签、属性和 ID 策略调整映射，不会自动枚举全图并提交迁移作业。

这组示例验证 `PRIMARY_KEY` 和 `CUSTOMIZE_NUMBER` 顶点 ID 策略。现有连接器对 `AUTOMATIC` 顶点会生成新 ID，不能用这组映射直接无损复制其边端点；迁移前先核对源图的 ID 策略。

验收应同时核对属性键类型、基数、标签、主键、边端点定义、索引和 Schema ID，以及全部顶点 ID/属性、边 ID/端点/属性；只比较数量或作业状态不够。相关选项见官方 [HugeGraph Source](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/source/HugeGraph.md) 和 [Sink](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/sink/HugeGraph.md)。

## 本轮实测记录

待填写：使用上述原生 hybrid 实现，在 HugeGraph Helm 1 PD + 1 Store + 1 Server/HStore 环境验证单副本、多副本、内置前端，并完成新环境 A → B 的 Schema、1000 个点与 2400 条边复制。旧版分离 master/worker 的实测不作为本实现的验证结果。
