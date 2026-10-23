# HugeGraph 与 SeaTunnel 联合部署

本 Chart 直接复用 [SeaTunnel 3.0.0 官方 hybrid Kubernetes 方案](https://github.com/apache/seatunnel/blob/v3.0.0/docs/zh/getting-started/kubernetes/hybrid-cluster-mode.md)，在 HugeGraph Helm 中适配资源名称、DNS、镜像、配置和副本数，不修改 SeaTunnel Helm，也不引入子 Chart、Operator 或 CRD。

SeaTunnel 默认关闭。启用后，一个常驻 hybrid 节点同时承担调度和执行，持续接收任务；集群模式只是把同一个 StatefulSet 扩为多个 hybrid 节点。它与执行完即退出的 `local-mode` Pod/Job 不同。

| 配置 | 含义 |
|---|---|
| `values-single.yaml` | HugeGraph 1 PD + 1 Store + 1 Server，使用 HStore |
| `values-cluster.yaml` | HugeGraph 自身的多副本拓扑 |
| `seatunnel.mode: standalone` | 一个常驻 hybrid 节点 |
| `seatunnel.mode: cluster`、`replicas: 2` | 两个 hybrid 节点，副本数至少为 2 |

HugeGraph 与 SeaTunnel 的拓扑独立选择；顶层 `mode: easy/strict` 控制同一 release 的 HTTP 入口和 NetworkPolicy 默认值。SeaTunnel 单机和集群均提供相同的内置前端与 API。

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

默认使用 `apache/seatunnel:3.0.0`。两种模式都开启引擎自带的 HTTP 服务和任务可视化前端，无需另部署 Web 服务或数据库。

SeaTunnel HTTP Service 沿用 HugeGraph 的访问模式；显式设置 `seatunnel.service.type` 时优先使用指定值：

| 配置 | HTTP Service 默认值 | 访问方式 |
|---|---|---|
| `mode: easy` | `NodePort` | 可达节点 IP 加分配的 NodePort |
| `mode: strict` | `ClusterIP`，开启 NetworkPolicy | 集群内访问，或由运维人员临时端口转发 |
| 显式 `seatunnel.service.type` | 使用指定的 `ClusterIP`、`NodePort` 或 `LoadBalancer` | 按实际 Service 地址访问 |

例如指定 NodePort；不设置 `nodePort` 时由 Kubernetes 分配：

```yaml
seatunnel:
  enabled: true
  service:
    type: NodePort
    nodePort: 30081
    annotations: {}
```

```bash
kubectl -n hugegraph get svc hugegraph-seatunnel \
  -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}{"\n"}'
kubectl get nodes -o wide
```

浏览器入口为 `http://<可达节点IP>:<NodePort>/#/overview`，API 使用同一地址的相应 REST 路径。使用 LoadBalancer 时，通过 `kubectl -n hugegraph get svc hugegraph-seatunnel -o wide` 查看分配的地址，再访问 `http://<负载均衡地址>:8080/#/overview`。kind、minikube 等本地环境需按其网络方式提供可达入口。

所有 Service 类型均可使用端口转发：

```bash
kubectl -n hugegraph port-forward svc/hugegraph-seatunnel 8081:8080
```

对应前端地址为 <http://127.0.0.1:8081/#/overview>，使用下节的独立 SeaTunnel 账号完成浏览器 Basic 登录。前端用于查看集群、作业、DAG 和日志；作业通过 SeaTunnel CLI 或受保护的 REST API 提交。Helm 只启动引擎并准备示例配置，不自动建图或执行作业。

外部入口只包含 HTTP 8080。Hazelcast 5801 仍仅通过内部 Headless Service 供成员发现及 CLI 通信，不随 NodePort 或 LoadBalancer 一起公开。strict 模式下，额外的应用、监控或入口代理需通过 `networkPolicy.seatunnel.extraIngress` 配置原生规则；本 Chart 不额外管理 SeaTunnel Ingress。

release 为 `hugegraph` 时，资源为 `hugegraph-seatunnel` StatefulSet、同名 HTTP Service、`hugegraph-seatunnel-headless` 发现 Service，以及 `hugegraph-seatunnel-configs` ConfigMap。名称遵循 HugeGraph 的 `fullnameOverride`；成员通过 Headless Service 的 DNS 发现，不访问 Kubernetes API，因此不需要发现用的 RBAC。

切换前先停止全部作业，保留安装使用的完整 values，再升级：

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph -f my-values.yaml \
  --set seatunnel.enabled=true --set seatunnel.mode=cluster \
  --set seatunnel.replicas=2 --wait --timeout 30m
```

切回单机设置 `seatunnel.mode=standalone`。两种模式使用同一个 StatefulSet，但副本和引擎配置会变化；不承诺无中断切换，也不会自动恢复或重新提交作业。设置 `seatunnel.enabled=false` 移除本 release 的 SeaTunnel 工作负载和 Service，已经写入 HugeGraph 的数据保留。认证 Secret 带有保留策略，禁用或卸载时仍会保留，便于再次启用时复用凭据。

## SeaTunnel 入口认证

前端、静态资源和 REST 提交接口默认使用引擎原生 HTTP Basic 认证，不新增认证代理或独立登录服务。SeaTunnel 使用独立入口管理员：`seatunnel.auth.username` 默认为 `admin`；密码未显式设置时，Chart 自动生成强密码并保存在 `<SeaTunnel资源名>-auth` Secret，升级时复用已有密码。

也可通过受控 values 文件显式设置 `seatunnel.auth.username/password`，用于自定义账号或轮换。自定义值不能有首尾空白或 `${...}` 占位符，用户名另不能包含冒号；内部空格和引号仍可使用。只做离线模板或 GitOps 渲染时，应显式固定 `seatunnel.auth.password`；默认密码的升级复用依赖在线 Helm lookup。

默认 release 的 Secret 为 `hugegraph-seatunnel-auth`。保持前述端口转发，在本机读取账号和密码后调用 REST：

```bash
ST_USERNAME="$(kubectl -n hugegraph get secret hugegraph-seatunnel-auth \
  -o jsonpath='{.data.username}' | base64 --decode)"
ST_PASSWORD="$(kubectl -n hugegraph get secret hugegraph-seatunnel-auth \
  -o jsonpath='{.data.password}' | base64 --decode)"

curl -fsS --user "$ST_USERNAME:$ST_PASSWORD" http://127.0.0.1:8081/overview
```

浏览器打开 `/#/overview` 时，使用 Secret 中的同一组账号密码完成 HTTP Basic 登录；访问其他 NodePort 或 LoadBalancer 地址也使用这组入口凭据。外部提交作业使用同一受保护 HTTP 入口，并按 [官方 REST API](https://github.com/apache/seatunnel/blob/v3.0.0/docs/zh/engines/zeta/rest-api-and-web-ui.md) 的格式携带认证信息。

入口 Secret 包含 `username`、`password` 及最终的 `seatunnel.yaml`；包含认证信息的引擎文件直接从 Secret 挂载，不进入 ConfigMap、Java 参数或日志。轮换时在受控 values 中指定新密码并执行 `helm upgrade`，同步更新 Secret 内容并滚动重启；只修改 Secret 的 `data.password` 不会同步修改其中的引擎文件。`HG_USERNAME/HG_PASSWORD` 是作业连接 HugeGraph 的另一组凭据，不会替代入口认证。即使 `server.auth.enabled=false`，SeaTunnel 入口仍使用独立认证。

HTTP Basic 不覆盖 Hazelcast 5801 的内部成员和 CLI 协议。下文在 Pod 内执行的 `seatunnel.sh --master cluster` 属于可信 Kubernetes/RBAC 管理范围；外部客户端应使用受保护的 REST 入口，不能把 5801 对外公开。本配置提供供可信管理人员使用的单一管理员入口，不提供按用户或作业划分的细粒度权限，也不复用 HugeGraph 的多用户权限、RBAC 或 SSO。需要对外服务时可接入已有 TLS 入口；本 Chart 不新增 Ingress 管理。

## 镜像、凭据与原生配置

本轮已直接核实官方 `apache/seatunnel:3.0.0` 镜像自带 HugeGraph、FakeSource、Console 连接器和内置 UI，直接复用即可，无需维护派生镜像。更换版本或自定义镜像时再核对插件与依赖。镜像通过 `seatunnel.image.repository/tag/pullPolicy` 选择，继承 HugeGraph 的 `global.imageRegistry` 和 `imagePullSecrets`；Helm 不在启动时下载插件。官方镜像包含大量连接器，本次 amd64 镜像压缩后约 3.7GB；首次拉取较慢时可延长安装 timeout，或预拉取、镜像到内部仓库。

启用 SeaTunnel 后，Chart 为同 release 的 HugeGraph 演示作业准备以下环境变量，无需手写 Server 地址或复制管理员密码：

| 变量 | 默认来源 |
|---|---|
| `HG_HOST` | 当前 release 的 HugeGraph Server Service DNS |
| `HG_PORT` | `server.port` |
| `HG_PROTOCOL` | `http`，访问集群内部 Server |
| `HG_USERNAME` | `admin` |
| `HG_PASSWORD` | 当前 Server 管理员 Secret 的实际名称与键；认证关闭时为空密码 |
| `HG_GRAPH_SPACE` | `server.graphSpace` |
| `HG_GRAPH_NAME` | `graph_a` |

`HG_PASSWORD` 使用原生 `secretKeyRef`，兼容 `server.auth.admin.existingSecret` 及其自定义键，不把密码写入演示文件。Chart 不因此创建额外的 HugeGraph 账号；更新密码 Secret 后，按 Server 账号同步流程处理，再重启相关 SeaTunnel Pod 刷新环境变量。

`seatunnel.env` 中同名条目显式覆盖自动值，最终每个变量只有一个有效条目。例如更换演示图，或使用已有的专用作业账号：

```yaml
seatunnel:
  env:
    - name: HG_GRAPH_NAME
      value: import_demo
    - name: HG_USERNAME
      value: seatunnel-job
    - name: HG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: hugegraph-job-credentials
          key: password
```

专用账号需预先存在，并具备所用图的数据和 Schema 权限。这组 `HG_*` 默认值只服务于本 release 的简单 Source/Sink 示例；跨环境复制仍由用户提供下文的 `HG_SOURCE_*`、`HG_TARGET_*`，不会自动发现或配置远端图。SeaTunnel 前端/API 自身的认证也不能由这组连接器变量替代。

`seatunnel.javaOpts` 调整引擎 JVM，`resources` 调整容器资源。高级配置保留 SeaTunnel 原生文件格式，通过 `seatunnel.config` 按文件名完整覆盖，不逐项增加 Helm 开关，例如：

```yaml
seatunnel:
  javaOpts: "-Xms512m -Xmx1g"
  config:
    jvm_client_options: |
      -Xms128m
      -Xmx256m
```

默认在 ConfigMap 中准备 `hazelcast.yaml`、`hazelcast-client.yaml`、JVM 和演示作业文件；包含入口认证的 `seatunnel.yaml` 从独立 Secret 挂载，其余配置沿用镜像。`seatunnel.config` 可覆盖引擎文件，但最终 HTTP Basic 开关和凭据始终由 `seatunnel.auth` 管理。覆盖发现或引擎文件时保留 TCP 5801、HTTP 8080 和 `enable-http: true`，才能继续使用现有 Service、探针及前端。默认 checkpoint 使用临时本地目录；本集成不提供持久化作业恢复保证。

strict 模式允许 SeaTunnel 成员通信、DNS，以及访问同 release 的 HugeGraph Server。远端图 A、其他数据源及调用方，通过 `networkPolicy.seatunnel.extraEgress/extraIngress` 补充原生规则；实际隔离依赖 CNI 支持。HugeGraph 的凭据、存储和访问方式见 [Chart README](../README.md)。

## 同 release 演示：直接导入并读取

Chart 启动时把 [hugegraph-sink.conf](../examples/seatunnel/hugegraph-sink.conf) 和 [hugegraph-source.conf](../examples/seatunnel/hugegraph-source.conf) 放入现有的 SeaTunnel ConfigMap，分别挂载为：

- `/opt/seatunnel/config/hugegraph-sink.conf`：`FakeSource → HugeGraph`，向 `seatunnel_demo_person` 标签写入十条固定数据。
- `/opt/seatunnel/config/hugegraph-source.conf`：读取同一图中的演示标签，输出到 Console。

两个文件均使用前述自动 `HG_*` 环境变量；无需上传临时文件或手动补全当前 Server 地址、端口、图空间及密码。需要调整作业时，可在 `seatunnel.config` 中按同名文件完整覆盖。

先通过 Hubble 或 HugeGraph API 创建 `graph_a`。如果改写了 `HG_GRAPH_SPACE/HG_GRAPH_NAME`，则创建对应图。以下命令按默认 release、命名空间及图空间演示建图；本机的 `HG_USERNAME/HG_PASSWORD` 由凭据管理器提供，Pod 环境变量不会传到本机：

```bash
# 在另一个终端保持 Server 端口转发
kubectl -n hugegraph port-forward svc/hugegraph-server 8082:8080
```

```bash
export HG_URL=http://127.0.0.1:8082
curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  -X POST "$HG_URL/graphspaces/DEFAULT/graphs/graph_a" -d '{}'
```

已有图不重复建图。随后直接提交已挂载的 Sink，再提交 Source：

```bash
kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster \
  --config /opt/seatunnel/config/hugegraph-sink.conf

kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster \
  --config /opt/seatunnel/config/hugegraph-source.conf
```

`--master cluster` 表示提交给常驻引擎，单节点也使用该参数。首次空图导入后，Sink 作业应为 `FINISHED`，读取、写入各十条；Source 应读取十条并输出到执行节点的日志。可先在前端查看作业状态和 DAG，再检查各 hybrid Pod 日志及图中实际数据：

```bash
kubectl -n hugegraph logs -l app.kubernetes.io/instance=hugegraph,app.kubernetes.io/component=seatunnel \
  --all-containers --prefix --tail=200

curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  "$HG_URL/gremlin" -d '{"gremlin":"graph.traversal().V().hasLabel(\"seatunnel_demo_person\").count()","aliases":{"graph":"DEFAULT-graph_a"}}'
```

数量查询的 `result.data` 应为 `[10]`。集群模式下作业可能由任一 hybrid 节点执行，因此 Console 输出不限定在 `hugegraph-seatunnel-0`。这两个演示作业也不会在 Helm 安装或升级时自动执行。

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

## 已完成实测记录：原生 hybrid 基线

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

上述基线还通过了 22 项 Python Helm 回归、309 项 Helm 单元测试（36 个 suite、4 个快照）、两种模式的 strict Helm lint，以及目标环境的 `helm test --logs`。`mvn editorconfig:format` 和 `mvn clean compile -Dmaven.javadoc.skip=true` 的 38 个模块均通过。

本次范围与已知限制：

- 已覆盖 `PRIMARY_KEY`、`CUSTOMIZE_NUMBER`，暂不覆盖 `AUTOMATIC` 顶点的无损复制；后者需要解决新旧顶点 ID 与边端点的对应关系。
- 官方 3.0.0 UI 的 Duration 显示存在上游问题：无时区时间字符串在新加坡浏览器中会产生 8 小时偏差，且已完成作业的显示时长仍随当前时间增长。以 CLI/REST 的完成状态及开始、结束时间核对实际结果；本次未修改上游 UI 或硬编码时区。
- 测试覆盖停止作业后的模式升级和数据保留，未验证运行中作业不中断切换、生产 HA、持久化恢复或 CNI 的实际网络隔离。默认临时 checkpoint 目录不能据此视为持久化恢复方案。

## 本轮增量验证记录

本轮新增 SeaTunnel HTTP 入口继承 easy/strict、独立原生 HTTP Basic 认证、同 release 的 `HG_*` 默认连接信息，以及启动时挂载 Source/Sink 演示文件。上一节的原生 hybrid 基线记录验证了引擎、跨环境 Schema／点／边复制及内置 UI，不作为这些新增行为的运行验证结论。

2026 年 10 月 24 日（Asia/Singapore）完成增量实测。使用一套独立的 **1 PD + 1 Store + 1 Server/HStore**，先保持 SeaTunnel 关闭，再同 release 开启，随后切换 strict 双节点并轮换入口密码。为验证配置来自 Helm 实际值，使用 `fullnameOverride: bridge`、Server REST 端口 `8184`，以及外部 HugeGraph 管理员 Secret `graph-admin` 的自定义键 `credential`；SeaTunnel 未手动设置任何 `HG_*`。

SeaTunnel 继续使用上节相同 digest 的官方原镜像；本轮重新下载并验证九层及配置、清单，在独立节点再次核对全部 SHA-256 和大小。HugeGraph 继续使用上节注明版本的缓存运行产物；当前分支已合入 remote master 的 `75ea5c7dc`，并另行通过全部 38 模块编译。

| 项目 | 实测结果 |
|---|---|
| easy 真实入口 | HTTP Service 自动继承 NodePort，指定 `32080`，经真实节点端口访问；只有 HTTP 对外公开，5801 保留内部 |
| strict 双节点 | Service 自动改为 ClusterIP、移除 NodePort，原 `32080` 入口实际不可访问；每个 Pod 均报告 Workers=2、Hazelcast Members=2，持续稳定超过一分钟 |
| 入口认证 | 每阶段检查首页、`index.html`、真实静态资源、`/overview` 及 `POST /submit-job`；匿名、错误用户名、错误密码、HugeGraph 管理员凭据共 60 个负例全部 401，正确 SeaTunnel 凭据返回 200；SeaTunnel 凭据访问 HugeGraph 同样被拒绝 |
| 默认连接与文件 | 自动连接 `bridge-server.<namespace>.svc:8184`，正确引用 `graph-admin/credential`；挂载的 Source/Sink 配置与仓库文件字节一致，无上传或手动补齐步骤 |
| Source/Sink | easy 单节点、strict 双节点分别直接运行两个挂载示例，均 `FINISHED`、读 10、写 10、失败 0；Source Console 中十组姓名及年龄与图数据逐条一致 |
| REST 提交 | 三阶段各通过带认证的 REST 提交一次 FakeSource → Console，均 `FINISHED`，读取、写入和提交计数均为 10 |
| 升级与轮换 | 普通在线升级复用独立密码；显式轮换后旧密码 401、新密码 200，Secret UID 与用户名保留。两个引擎 Pod 滚动更新后仍稳定为双成员 |
| HugeGraph 保持 | 启用 SeaTunnel、切换 strict 双节点和密码轮换后，原 3 个 HugeGraph Pod UID、2 个 PVC、3 个原有凭据 Secret 均未变，Pod Ready/0 重启，图仍为准确的 10 个顶点、0 条边；启用前及 strict 后原生 `helm test` 均通过 |
| 凭据存放 | 原生认证配置与独立 Secret 一致，`seatunnel.yaml` 只从 Secret 挂载，认证 checksum 正确；SeaTunnel 密码未进入 ConfigMap、Pod 环境变量或 JVM 参数。业务及浏览器证据中的实际密码、Basic token 扫描均无泄漏 |

七个实际作业的记录如下：

| 阶段 | 作业 | ID | 状态 / 读写 |
|---|---|---|---|
| easy 单节点 | 挂载 Sink | 1166060683054284801 | FINISHED / 10、10 |
| easy 单节点 | 挂载 Source | 1166060702146756609 | FINISHED / 10、10 |
| easy 单节点 | REST FakeSource → Console | 1166060687785721857 | FINISHED / 10、10 |
| strict 双节点 | 挂载 Sink | 1166061637581406210 | FINISHED / 10、10 |
| strict 双节点 | 挂载 Source | 1166061657428852738 | FINISHED / 10、10 |
| strict 双节点 | REST FakeSource → Console | 1166061642451255297 | FINISHED / 10、10 |
| 轮换后双节点 | REST FakeSource → Console | 1166062548156416001 | FINISHED / 10、10 |

真实 Chromium 使用新加坡时区，分别在 01:09:12、01:13:01、01:16:46 完成 easy 单节点、strict 双节点、轮换后双节点的 Basic 登录、Overview、Jobs 和 DAG 验证，Workers 分别为 1、2、2。每次选中作业的 DAG 都有两个节点、一条连接，页面显示读写各 10；累计 21 次关键 API、51 次静态资源响应全部 200，JS、控制台及请求错误均为 0。九张截图均人工检查，未打开可能展示作业凭据的配置页。

预热时曾选择 REST 端口 `8182`，与 HugeGraph 默认 Gremlin 端口冲突；改为 `8184` 并通过原生 Helm test 后才建立资源基线。strict 后从未获放行的 Server Pod 探测 SeaTunnel 被阻断，最终成员检查改为逐引擎 Pod 本地认证访问；没有为测试扩大默认放行范围。

最终本地回归为 30 项 Python 测试（含 15 项 SeaTunnel 集成）、309 项 Helm 单元测试，formatter 和最新 master 基础的 38 模块编译通过。CI 发现的旧 release values 缺少 `seatunnel` 字段兼容问题已补上空值处理，并新增真实旧 values 的安装、升级及自定义镜像仓库渲染回归。

验证结束后已清理本轮专用 kind 集群、网络及卷，日志、结果和截图保留。原有测试环境的节点身份、启动时间及运行状态均未变化。

验证结束后已清理本轮专用 kind 集群、网络及卷，日志、结果和截图保留。原有测试环境的节点身份、启动时间及运行状态均未变化。
