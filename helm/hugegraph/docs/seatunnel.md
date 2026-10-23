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
  --wait --timeout 30m

# 同样的 HugeGraph 拓扑，加两个 SeaTunnel hybrid 节点
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-cluster.yaml \
  --wait --timeout 30m
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
  --set seatunnel.replicas=2 --wait --timeout 30m
```

切回单机设置 `seatunnel.mode=standalone`。两种模式使用同一个 StatefulSet，但副本和引擎配置会变化；不承诺无中断切换，也不会自动恢复或重新提交作业。设置 `seatunnel.enabled=false` 移除本 release 的 SeaTunnel，已经写入 HugeGraph 的数据保留。

## 镜像、凭据与原生配置

本轮已直接核实官方 `apache/seatunnel:3.0.0` 镜像自带 HugeGraph、FakeSource、Console 连接器和内置 UI，直接复用即可，无需维护派生镜像。更换版本或自定义镜像时再核对插件与依赖。镜像通过 `seatunnel.image.repository/tag/pullPolicy` 选择，继承 HugeGraph 的 `global.imageRegistry` 和 `imagePullSecrets`；Helm 不在启动时下载插件。官方镜像包含大量连接器，本次 amd64 镜像压缩后约 3.7GB；首次拉取较慢时可延长安装 timeout，或预拉取、镜像到内部仓库。

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

作业使用 `ERROR_WHEN_SCHEMA_NOT_EXIST`，复用已经复制的 Schema；边依赖先前写入的端点顶点。这组映射覆盖 [person](../examples/seatunnel/clone/person.conf)、[company](../examples/seatunnel/clone/company.conf)、[works_at](../examples/seatunnel/clone/works_at.conf) 和 [knows](../examples/seatunnel/clone/knows.conf)。官方 Sink 要求显式提供 `mappings` 或 `schema_config`。复制其他图时需按实际标签、属性和 ID 策略调整原生映射，不会自动枚举全图并提交迁移作业。

这组示例验证 `PRIMARY_KEY` 和 `CUSTOMIZE_NUMBER` 顶点 ID 策略。现有连接器对 `AUTOMATIC` 顶点会生成新 ID，不能用这组映射直接无损复制其边端点；迁移前先核对源图的 ID 策略。

验收应同时核对属性键类型、基数、标签、主键、边端点定义、索引和 Schema ID，以及全部顶点 ID/属性、边 ID/端点/属性；只比较数量或作业状态不够。相关选项见官方 [HugeGraph Source](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/source/HugeGraph.md) 和 [Sink](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/sink/HugeGraph.md)。

## 本轮实测记录

2026 年 10 月 23–24 日（Asia/Singapore）完成以下验证，均使用本页的一套原生 hybrid StatefulSet。源、目标是两个独立的 Helm 环境，各有 **1 PD + 1 Store + 1 Server，后端为 HStore**，分别使用自己的 PVC 和认证。源 `graph_a` 与目标 `graph_b` 不共享后端；另在目标环境的 `graph_a` 测试十条数据直接导入。

SeaTunnel 使用原始 `apache/seatunnel:3.0.0` 镜像。实际校验了 OCI 清单、配置及全部九层的大小和 SHA-256，并在容器中确认 HugeGraph、FakeSource、Console 连接器及内置 UI 均存在；无需自建镜像。镜像索引 digest 为 `sha256:914fcf37b2d5acdf9f95f7dbbd7d4c62d150fcbc5ff3e5f59957d65840fea8dd`，实际运行 amd64 清单 digest 为 `sha256:dd8f357dfed2192a68184aeac41507e5529eb06c208b12aae4bf6466ed0a0bb0`。

测试环境资源有限，kind 节点使用 FUSE overlay 承载官方镜像原始层；PD、Store、Server 则复用此前从源码构建的缓存运行镜像。以上均为测试基础设施设置，不是 Chart 的部署依赖。HugeGraph 测试运行产物的源码版本为 `f60ad3e3bfc71d4404956fd2dbfb62c7bc002f13`；本轮同时对最新 `master` 基础上的改动完成了编译检查。

| 验证项 | 实际结果 |
| --- | --- |
| 直接导入 | 原始 `hugegraph-sink.conf` 未修改，作业 `1166045557131575297` 为 `FINISHED`，读 10、写 10、失败 0；目标图查询及逐条内容核对均为 10 条 |
| Schema 复制 | 空目标先复制 10 个属性键、2 个顶点标签、2 个边标签、3 个索引；保留原 Schema ID，逐项核对类型、基数、ID 策略、nullable、索引和自定义元数据 |
| 单机复制 | 00:08:49 完成，1 个 hybrid 节点；4 个作业全部 `FINISHED`，1000 个顶点、2400 条边的 ID、全部属性和边端点与源图相同 |
| 集群复制 | 00:18:51 完成，2 个 hybrid 节点；人为扰动目标一个属性后重新运行 4 个作业，目标恢复到与源图完全一致；每个 Pod 在作业前后均报告 Workers=2 |
| 切回单机 | 00:21:16 完成只读复查，Workers=1，运行中作业为 0；全部 Schema、1000 个顶点、2400 条边及另图的 10 条导入数据保持一致 |
| 升级影响 | 单机 → 集群 → 单机使用同一个 StatefulSet；两环境原有 6 个 HugeGraph Pod UID、4 个 PVC、6 个认证 Secret 均未变化，Pod 全部 Ready 且重启为 0；匿名访问 401、有效账号 200 |
| 内置前端 | 真实 Chromium（Asia/Singapore）验证两种模式的 Overview、作业列表和 DAG：Workers 分别为 1、2；选中作业的 DAG 均为 2 节点、1 条连接，显示读写各 800；浏览器 JS、关键 API、静态资源错误均为 0 |

复制作业的逐项结果如下；两种模式每个作业的失败数均为 0。

| 标签 | 类型 | 读取 / 写入 | 单机作业 ID | 集群作业 ID |
| --- | --- | --- | --- | --- |
| person | PRIMARY_KEY 顶点 | 800 / 800 | 1166045485111181313 | 1166048035369648130 |
| company | CUSTOMIZE_NUMBER 顶点 | 200 / 200 | 1166045504388202497 | 1166048053988163586 |
| works_at | 边 | 800 / 800 | 1166045519504474113 | 1166048070853459969 |
| knows | MULTIPLE 边 | 1600 / 1600 | 1166045539670687745 | 1166048087110582273 |

验收使用全量数据比较，单机、集群和切回单机后的 Schema、顶点、边三组规范化哈希分别完全一致。Schema 比较仅忽略元素顶层的运行状态及系统 `user_data.~create_time`，保留业务自定义的 `status/create_time` 元数据。另验证了 SET 内容、LIST 顺序和重复元素、可空属性、自定义元数据、同端点且 sort key 为 2020/2021 的两条多重边；三个索引均为 `CREATED`，范围查询 `age >= 80` 返回 88 条，二级索引查询 `company_123` 返回 1 条，源、目标查询结果相同。

集群测试曾发现两个 Pod 虽然 Ready，却各自形成单节点集群。原因是 Hazelcast DNS 发现未指定端口时使用 5701，而引擎监听 5801；模板已明确配置 `service-port: 5801`。修复后逐 Pod 确认 Workers=2、成员数为 2，持续观察一分钟，再重新执行上述集群作业和前端验证。仅 Pod Ready 不作为集群组建成功的依据。

本轮还通过了 22 项 Python Helm 回归、309 项 Helm 单元测试（36 个 suite、4 个快照）、两种模式的 strict Helm lint，以及目标环境的 `helm test --logs`。`mvn editorconfig:format` 和 `mvn clean compile -Dmaven.javadoc.skip=true` 的 38 个模块均通过。

本次范围与已知限制：

- 已覆盖 `PRIMARY_KEY`、`CUSTOMIZE_NUMBER`，暂不覆盖 `AUTOMATIC` 顶点的无损复制；后者需要解决新旧顶点 ID 与边端点的对应关系。
- 官方 3.0.0 UI 的 Duration 显示存在上游问题：无时区时间字符串在新加坡浏览器中会产生 8 小时偏差，且已完成作业的显示时长仍随当前时间增长。以 CLI/REST 的完成状态及开始、结束时间核对实际结果；本次未修改上游 UI 或硬编码时区。
- 测试覆盖停止作业后的模式升级和数据保留，未验证运行中作业不中断切换、生产 HA、持久化恢复或 CNI 的实际网络隔离。默认临时 checkpoint 目录不能据此视为持久化恢复方案。
