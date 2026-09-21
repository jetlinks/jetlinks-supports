# DeviceRegistry 已校验设备缓存优化

## 目标与范围

- owning module：`jetlinks-supports`
- 优化入口：`ClusterDeviceRegistry.getDevice`
- 目标：设备一级缓存完成一次存在性校验后直接标量返回，减少产品解析和 Reactor Subscriber 分配。
- 保持设备不存在、配置变更、设备/产品注销、错误、取消和 Context 语义。

## 不做

- 不修改配置值缓存、二级缓存、回源和集群通知协议。
- 不增加每设备事件订阅、产品到设备反向索引或新的结果缓存。
- 无配置变更通知能力的 `ConfigStorageManager` 继续使用原逐次校验逻辑。

## 实施步骤

1. 增加可失效的 `MonoValidatedDeviceOperator`：首次订阅校验，稳定命中使用 `ScalarSubscription`。
2. 为 `EventBusStorageManager` 增加管理器级通知监听能力，本地和远程通知使用同一回调。
3. 设备产品绑定变化时失效对应设备缓存；产品存在性变化时清理产品及设备一级缓存。
4. 增加首次校验、快速命中、失效重校验、空结果、并发和通知测试。
5. 使用独立 JVM 对比 QPS、B/次和 JFR 热点，并验证 10 万设备常驻内存。

## 风险与验证

- 重点验证“失效发生在校验期间”不会将旧 Operator 永久写回缓存。
- 重点验证跨节点注销后不会继续稳定返回已注销设备。
- 性能收益以多轮交错 A/B 的分配和 QPS 为准；若吞吐区间重叠或常驻内存明显增加，则回退实现。

## 结果

- 新增 `MonoValidatedDeviceOperator`：首次订阅沿用 Reactor 原生链完成存在性校验，校验版本有效时使用 `Operators.scalarSubscription` 返回。
- `EventBusStorageManager` 提供管理器级缓存通知监听；本地通知、远程通知和 `refreshAll()` 都会失效对应设备校验状态，不增加每设备订阅。
- 设备产品绑定变化仅失效对应设备；产品协议变化、产品 clear、产品注销会清理产品缓存并失效设备一级缓存。
- 无通知能力的 `ConfigStorageManager` 保持原逐次校验逻辑，不改变兼容语义。

### 正确性验证

```bash
mvn clean package \
  -Djacoco.skip=true \
  -Dtest=MonoValidatedDeviceOperatorTest,ClusterDeviceRegistryTest,EventBusStorageManagerTest
```

- 18 tests，0 failure，0 error，构建成功。
- 覆盖首次校验、稳定快路径、demand、cancel、Context、error 重试、失效重校验、空结果清除、并发包装竞争、校验期间失效、设备绑定变化、产品移除、无通知能力兼容、本地通知和 `refreshAll()` 失效。

### 性能结果

独立 JVM，JDK 17.0.18，Reactor 3.7.8，G1，4 线程，预热 5 秒、测量 8 秒，最终产物 3 轮交错 A/B：

| 版本 | QPS 中位数 | B/次 | 远程读取 |
|---|---:|---:|---:|
| 原逐次产品校验 | 74.14M | 256 | 0 |
| 已校验设备快路径 | 399.56M | 32 | 0 |

- QPS 提升 **438.9%**，分配减少 **87.5%**。
- 更长的 5 轮交错结果为 68.61M → 401.05M QPS，提升 **484.6%**，结论一致。

并发扩展中位数：

| 线程 | 基线 QPS | 优化后 QPS | 提升 |
|---:|---:|---:|---:|
| 1 | 22.20M | 127.62M | +474.9% |
| 4 | 72.02M | 327.73M | +355.0% |
| 8 | 97.94M | 608.05M | +520.8% |
| 16 | 146.69M | 813.61M | +454.7% |

JFR 显示基线热点集中在 `MonoDeviceProduct.resolve`、`LocalCacheClusterConfigStorage.getConfigs`、`MonoMap` 和多订阅切换；优化后分配只剩 32 B/次的 `Operators$ScalarSubscription`。

10 万缓存 entry、5 轮独立 JVM 常驻内存中位数：197.30 → 205.30 B/entry，增加 8 B/entry（+4.1%），约每 10 万设备增加 0.76 MiB。该成本未随订阅次数增长，远低于热路径分配减少带来的收益。

性能数据为同步 L1 命中的微基准，用于比较实现开销，不代表包含网络、业务处理和调度的端到端设备接口 QPS。

### 证据位置

- 最终 QPS：`target/device-api-jfr/validated-device-final-qps/`
- 5 轮长测：`target/device-api-jfr/validated-device-long-qps-v2/`
- 并发扩展：`target/device-api-jfr/validated-device-concurrency/`
- JFR：`target/device-api-jfr/validated-device-jfr/`
- 常驻内存：`target/device-api-jfr/validated-device-memory/`

### 交付

- 实现提交：`ebbffb2`
- Pull Request：https://github.com/jetlinks/jetlinks-supports/pull/43

## PR 并发评审修复计划

- 目标：阻止产品缓存失效后的旧加载结果回填，并确保设备快路径在订阅后延迟请求时不会越过已完成的失效。
- 范围：`ClusterDeviceRegistry` 的产品缓存写入/失效，`MonoValidatedDeviceOperator` 的订阅/请求边界及对应并发回归测试。
- 不做：不修改配置缓存、通知协议、产品/设备查找规则或常驻缓存形态。
- 步骤：先增加可控交错测试复现检查与写入、订阅与请求之间的竞态；再在产品缓存写入与失效处建立原子边界，并在设备快路径的实际发射点核对失效状态。
- 风险：保持 Reactor demand、cancel、Context、无通知能力管理器的原行为；产品缓存命中路径不引入互斥，设备快路径额外状态须测量分配和吞吐。
- 验证：先运行新增测试确认修复前失败，再运行目标集群注册/设备 Mono 测试及主代码编译；与既有基准比较热路径分配/QPS，记录残余风险。

### 并发评审修复结果

- 修复前新增的三个受控交错用例均失败：无版本和带版本产品在失效清空后回填，以及设备在订阅后、请求前失效仍发射旧 Operator。
- 产品缓存只在缺失加载的写入、按版本移除和失效清空处共用互斥边界；缓存命中查询不加锁。移除产品时推进加载代数，避免注销后仍在途的产品查询回填。
- 已校验设备在 `request` 时复核代数；若订阅后已失效，改走原存在性校验源，并转发 demand、cancel、Context、错误和空结果。每次快路径订阅多一个请求感知状态对象；不新增每设备常驻缓存。
- Maven 定向执行 `MonoValidatedDeviceOperatorTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest`：27 tests，0 failure，0 error；同时修正该测试中原有的 `Sinks.Empty#then` 编译错误。`git diff --check` 通过。IDE 项目未打开此仓库，Maven 编译和测试为本次有效验证。
- 原文中的 QPS/B-op 是修复前数据，**不作为修复后的性能结论**。本次没有同环境 A/B 压测；快路径每次订阅的额外分配及线上吞吐仍需另行测量。
