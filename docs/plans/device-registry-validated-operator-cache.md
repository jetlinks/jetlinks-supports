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

- 新增 `MonoValidatedDeviceOperator`：首次订阅沿用 Reactor 原生链完成存在性校验，校验版本有效时使用请求感知的自定义订阅返回。
- `EventBusStorageManager` 提供管理器级缓存通知监听；本地通知、远程通知和 `refreshAll()` 都会失效对应设备校验状态，不增加每设备订阅。
- 设备产品绑定变化仅失效对应设备；产品协议变化、产品 clear、产品注销会清理产品缓存并失效设备一级缓存。
- 无通知能力的 `ConfigStorageManager` 保持原逐次校验逻辑，不改变兼容语义。

### 正确性验证

```bash
mvn clean package \
  -Djacoco.skip=true \
  -Dtest=MonoValidatedDeviceOperatorTest,ClusterDeviceRegistryTest,EventBusStorageManagerTest
```

- 27 tests，0 failure，0 error，构建成功。
- 覆盖首次校验、稳定快路径、demand、cancel、Context、error 重试、失效重校验、空结果清除、并发包装竞争、校验期间失效、设备绑定变化、产品移除、无通知能力兼容、本地通知和 `refreshAll()` 失效。

### 缓存替换递归修复

- 目标：修复同一设备的两个 `MonoValidatedDeviceOperator` 在并发缓存替换时相互递归订阅并触发 `StackOverflowError`。
- 范围：仅调整 `MonoValidatedDeviceOperator` 的缓存委托入口和对应回归测试；不修改缓存结构、失效策略、存在性校验或 Registry 查找逻辑。
- 实现：订阅时只解析一次缓存快照；快照为另一个已校验设备 Mono 时直接进入其实际订阅逻辑，不再递归查询缓存。普通缓存 `Mono` 仍按 Reactor 标准订阅。
- 风险：并发替换期间单次请求使用已读取的合法缓存快照，不继续追逐绝对最新值；后续请求仍读取当前缓存，保持最终收敛。
- 验证：覆盖缓存条目持续交替、普通 `Mono` 委托、错误、取消和 Context 传播，并运行设备缓存与 Registry 定向测试。
- 结果：修复前缓存条目持续交替用例稳定触发 `StackOverflowError`，修复后相关 4 个测试类共 38 项全部通过，0 failure、0 error；`git diff --check` 通过。
- 交付：实现提交 `5e1ea80`，Pull Request：https://github.com/jetlinks/jetlinks-supports/pull/44

### 递归修复后分支覆盖计划

- 目标：补充缓存首次读取为空、`putIfAbsent` 竞争、被委托实例延迟 demand 前失效、空校验以及普通缓存 Publisher 的回归覆盖，确认修复未遗漏另一条缓存委托入口。
- 范围：重点验证缓存中的 `MonoValidatedDeviceOperator` 经 `MonoOnAssembly`、`hide` 等包装后仍不会重新递归查询缓存；同时覆盖兼容构造方法。除非新增用例能够稳定复现生产代码缺陷，否则不修改缓存结构、过期策略和 Registry 查找逻辑。
- 实现约束：不能依赖 `instanceof` 解包或跳过包装操作符；使用一次性委托标记让包装链最终订阅到目标实例时直接进入实际订阅逻辑，并在该边界移除标记，保持用户 Context、assembly hook 和中间操作符语义。
- 验证：与 `ConcurrentValidatedDeviceCacheTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest` 统一执行，并检查 demand、cancel、Context、错误和空结果语义。

### Assembly 包装修复与验证结果

- 根因：`subscribeCached` 通过 `instanceof MonoValidatedDeviceOperator` 判断是否直接进入实际订阅；缓存值经过 `MonoOnAssembly`、`hide` 或其他正常包装后类型判断失效，包装链再次进入同一缓存查找并形成递归。
- 修复：不再依赖缓存 Publisher 的运行时类型。委托订阅通过 Reactor Context 携带按“缓存实例 + deviceId”限定的循环标记；包装链最终到达目标实例时直接进入 `subscribeResolved`，并在校验器订阅前恢复原 Context。嵌套不同缓存的同名设备不会误命中，用户 Context 与包装操作符语义保持不变。
- 性能边界：新增对象只发生在多个未缓存实例竞争后需要委托给既有缓存 Publisher 的慢路径；Registry 正常缓存命中仍直接返回缓存对象，不增加高频快路径分配。
- 分支覆盖：新增 Assembly/debug hook、`hide`、`putIfAbsent` 竞争失败、延迟 demand 前失效、普通 Publisher、跨缓存同 deviceId、Context 隔离及废弃构造器场景。
- 定向验证：`MonoValidatedDeviceOperatorTest`、`ConcurrentValidatedDeviceCacheTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest` 共 43 项通过，0 failure、0 error。
- 全量验证：`mvn test` 执行 226 项，其中 2 项失败；`JsonSchemaTypeMapperTest#testMapFromProperty_NullValueType` 与 `DetailErrorMapperTest#testRoundTripConversion` 已在未修改基线 `e9e2420` 单独复现，确认与本次变更无关。`git diff --check` 通过。提交与 Pull Request：`pending`。

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

- 目标：阻止产品缓存失效后的旧加载结果回填，并确保设备快路径在订阅后延迟请求时不会越过已完成的失效；允许与失效并发、已通过请求时版本检查的发射使用旧设备。
- 范围：`ClusterDeviceRegistry` 的产品缓存写入/失效，`MonoValidatedDeviceOperator` 的订阅/请求边界及对应并发回归测试。
- 不做：不修改配置缓存、通知协议、产品/设备查找规则或常驻缓存形态。
- 步骤：保留产品缓存写入与失效的原子边界和设备请求时版本复核；取消快路径在下游回调期间持有设备锁，改测失效与发射并发、完成失效后的下一次请求。
- 风险：同一时刻已经通过校验的多个请求可能各返回一次旧设备，不能承诺固定时间窗口；保持 Reactor demand、cancel、Context、无通知能力管理器的原行为。
- 验证：先运行新增测试确认修复前失败，再运行目标集群注册/设备 Mono 测试及主代码编译；与既有基准比较热路径分配/QPS，记录残余风险。

### 并发评审修复结果

- 修复前新增的三个受控交错用例均失败：无版本和带版本产品在失效清空后回填，以及设备在订阅后、请求前失效仍发射旧 Operator。
- 产品缓存只在缺失加载的写入、按版本移除和失效清空处共用互斥边界；缓存命中查询不加锁。移除产品时推进加载代数，避免注销后仍在途的产品查询回填。
- 已校验设备在 `request` 时复核代数；若订阅后已失效，改走原存在性校验源，并转发 demand、cancel、Context、错误和空结果。失效与已通过校验的发射并发时允许旧结果，不在下游 `onNext` / `onComplete` 期间持有设备锁；不新增每设备常驻缓存。
- Maven 定向执行 `MonoValidatedDeviceOperatorTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest`：28 tests，0 failure，0 error；同时修正该测试中原有的 `Sinks.Empty#then` 编译错误。新增交错用例验证失效不等待正在运行的下游回调、失效后的下次请求重新校验。`git diff --check` 通过。
- 原文中的 399.56M QPS / 32 B-op 是更早版本、不同基准口径的数据，**不作为本次优化的性能结论**。

### 放宽并发失效语义后的性能复核

在确认原先存在其他压力进程后重新测量：开始前采样 CPU 约 84% 空闲，未发现仍在运行的压力测试。使用同一临时 JMH 基准比较加锁版 `b4fe0c4` 与当前无锁快路径：JDK 21.0.10、G1、512 MiB heap、每版本 2 fork、每 fork 2 次 1 秒预热和 3 次 2 秒测量；每次从 L1 缓存取 Mono 并订阅复用的 Subscriber。

| 同一热设备 | 加锁版 QPS | 无锁版 QPS | 本轮提升 | B/次（两版） |
|---|---:|---:|---:|---:|
| 1 线程 | 76.10M | 82.12M | 约 8% | ≈32 |
| 4 线程 | 12.07M | 24.19M | 约 100% | ≈32 |
| 8 线程 | 12.15M | 15.50M | 约 28% | ≈32 |

同一热设备的每个 fork 均显示无锁版吞吐更高，分配未增加；4 线程无锁版跨迭代波动明显，不能把约 100% 视为稳定生产收益。另测每线程独立设备和 L1 缓存时跨 fork 差异极大：4 线程两版各约 54–112M，8 线程加锁版约 139–404M、无锁版约 137–166M，不能判断多设备场景的收益或回退；该隔离模式也不等价于真实 Registry 的共享缓存。基准仅测本地缓存与订阅，不推断端到端设备上报吞吐。

### 10 万设备分布复核

在**同一个 Guava 缓存**预置 10 万个不同设备 ID 及已校验 Mono，每个线程以步长 113 循环遍历全部 ID；缓存未命中则基准失败。沿用上述 JDK、堆、预热、测量和 2 fork 配置，交错运行加锁版与无锁版。使用同一个模拟 DeviceOperator 与产品校验源隔离查找/订阅成本，未包含真实设备业务处理。`plain` 是无过期的强引用缓存；`expireSoft` 对应旧构造器中的软引用、访问后 30 分钟过期配置。

| 线程 | plain 加锁 / 无锁 QPS | expireSoft 加锁 / 无锁 QPS | plain / expireSoft B/次 |
|---:|---:|---:|---:|
| 1 | 6.00M / 7.42M | 7.36M / 6.69M | ≈32 / ≈80 |
| 4 | 18.43M / 23.78M | 5.67M / 4.59M | ≈32 / ≈80 |
| 8 | 33.29M / 48.32M | 5.27M / 5.53M | ≈32 / ≈80 |
| 16 | 34.50M / 53.74M | 6.33M / 5.31M | ≈32 / ≈80 |

`plain` 下 8 线程无锁版较加锁版约快 45%，且从 1 到 8 线程总吞吐增长；16 线程与部分其他组合的 fork 差异较大，不宜外推精确增幅。`expireSoft` 下两版均在约 5–7M QPS，增加线程没有有效扩展，当前不能认为无锁化改善了该缓存配置。该模式每次命中还维护访问时间和 recency queue；8 线程 JFR 分配样本以 `ConcurrentLinkedQueue$Node` 和 `ValidatedSubscription` 为主，CPU 样本集中在 `String.hashCode`、`LocalCache.connectAccessOrder` 和 `ConcurrentLinkedQueue.offer`。这说明缓存访问维护成本会盖过设备 Mono 的锁优化；不能仅凭同一热设备基准推断不同缓存配置下的真实收益。

## 可选并发设备缓存计划

- 目标：新增接收 `Duration` 访问空闲过期时间的构造器，使有配置变更通知能力的 Registry 使用 `ConcurrentHashMap` 承载已校验设备；保持现有 Guava 注入构造器和无通知管理器的逐次校验兼容路径。
- 范围：`ClusterDeviceRegistry`、`MonoValidatedDeviceOperator`、新的设备缓存承载类及相关测试；不修改配置值缓存、通知协议和产品缓存。
- 步骤：缓存读使用 map 查找与低频访问时间采样；共享调度器按周期扫描并条件删除空闲 entry；在注册、通知失效、注销和释放时保留版本失效与生命周期语义。
- 风险：定期扫描意味着空闲过期允许扫描周期内的延迟；强引用缓存与原软引用行为不同，必须记录常驻内存及清理后的引用释放。无通知管理器继续使用原校验路径。
- 验证：定向测试过期、续期、并发失效和 dispose；使用 10 万设备、单线程及并发 JMH 与原实现对照，并记录分配和跨 fork 波动。后续在本节回填结果。

### 实施与验证

- 新构造器 `ClusterDeviceRegistry(..., Duration expireAfterAccess)` 在支持配置变更通知时使用 `ConcurrentValidatedDeviceCache`；原缓存注入构造器保持不变。无通知能力的管理器仍使用原 Guava 访问过期缓存与逐次产品校验。
- 设备命中使用 `ConcurrentHashMap` 查找；每秒更新一次共享时间刻度并采样设备访问时间，空闲条目按 `max(1 秒, min(过期时间, 1 分钟))` 间隔扫描，扫描时复核访问时间并按当前 entry 条件删除。过期存在采样与扫描周期内的延迟。注册、产品/设备配置通知、注销及 `dispose()` 继续触发原有失效边界。
- 新模式使用强引用：必须按设备数量和实际 Operator 体积评估内存，不能直接等同于旧 `softValues()` 在内存压力下的回收能力。空闲过期并不是硬性最大容量。
- JDK 21.0.10、G1、512 MiB，10 万不同 ID，命中率 100%，JMH 每模式 2 fork、每 fork 2×1 秒预热与 3×2 秒测量；1 秒周期维护时间刻度，30 分钟过期策略，不含设备业务或网络调用：

| 缓存模式 | 1 线程 QPS | 8 线程总 QPS | B/次 |
|---|---:|---:|---:|
| Guava 普通强引用 | 10.75M | 22.37M | 32 |
| Guava 软引用＋访问过期 | 8.39M | 5.47M | 80 |
| CHM＋定期过期 | 17.93M | 137.14M | 32 |

CHM 相比普通 Guava 约为 1.67× / 6.13×，相比软引用访问过期约为 2.14× / 25.07×（单线程 / 8 线程）。CHM 两个 fork 在 8 线程下分别为 133.15M、141.12M；普通 Guava 分别为 21.09M、23.65M。仅表示满命中 L1 微基准的收益；强引用常驻内存、过期扫描及真实设备处理链路仍须按实际部署观察。

定向运行 `ConcurrentValidatedDeviceCacheTest`、`MonoValidatedDeviceOperatorTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest` 共 34 项，0 failure、0 error；`git diff --check` 通过。独立 JMH 源码和 JSON 保留在本机 `/private/tmp/ChmValidatedOperatorJmh.java` 与 `/private/tmp/jetlinks-map-maintenance-{1,8}.json`，非仓库提交文件。

### 百万设备及未知设备复核

沿用 30 分钟过期、100% 缓存命中的同一 JMH 操作，改为 100 万个不同 ID、2 GiB heap、1/8 线程各 2 fork（每 fork 2×1 秒预热、3×2 秒测量）：

| 缓存模式 | 1 线程 QPS | 8 线程总 QPS | B/次 |
|---|---:|---:|---:|
| Guava 普通强引用 | 4.04M | 18.74M | 32 |
| Guava 软引用＋访问过期 | 2.21M | 5.75M | 80 |
| CHM＋定期过期 | 4.48M | 38.70M | 32 |

百万设备下 CHM 8 线程约为普通 Guava 的 2.07 倍、旧软引用过期配置的 6.73 倍；相对 10 万设备场景，容量增大后其优势缩小。这里复用模拟 Operator，只测本地查找和订阅，不代表完整设备上报。JMH JSON 在本机 `/private/tmp/jetlinks-map-million-{1,8}.json`。

独立 JVM（JDK 21、G1、`-Xms128m -Xmx4g`）用 100 万个不同 ID、**每 ID 一个真实 `DefaultDeviceOperator`** 与共享的模拟存储/产品校验源测 GC 后保留堆；时钟可控地推进 20/31/51 分钟验证空闲清理。3 轮独立进程的保留堆增量均约为：100 万 entry **580 MiB**（约 608 B/entry）；再读取 100 万个不同的不存在设备并完成空结果清除后，entry 仍为 100 万，保留堆不变；清除 90 万冷 entry、保留 10 万热 entry 后约 **66.7 MiB**；全部过期后仍比空缓存多 **10.25 MiB**（主要是 CHM 已扩容的表）。清理 90 万 entry 用 65–77 ms，余下 10 万用约 10 ms。这是共享模拟存储的 Operator 对象图，真实配置存储和元数据可能增加保留量。

另一独立 JVM 对实际 `ClusterDeviceRegistry.getDevice` 用 8 线程访问 100 万个不同未知设备，校验源返回空：结束时设备缓存 **0 entry**，GC 后堆相对基线约 **+0.25 MiB**；约 5.7–6.2M QPS 是该模拟存储探针的总吞吐，不与满命中 JMH 直接比较。诊断源码在本机 `/private/tmp/MillionDeviceCacheMemory.java`。

**RSS 与保留堆不同**：真实 Operator 三轮中，全部清理并 GC 后，堆已用约 23 MiB、已提交 128 MiB，但 `ps` 读取的进程 RSS 仍约 1.25 GiB；未知设备单独探针也出现未保留 entry 而 RSS 较高的情况。不能把这些短时 RSS 视为缓存仍持有设备；整桶淘汰也不保证 JVM/操作系统立即降低 RSS。线上内存风险需结合 GC、进程工作集及实际存储实现监测。

同口径交付复核：JDK 21.0.10、G1、2 GiB heap、8 线程、3 fork，每 fork 2×1 秒预热、3×2 秒测量，30 分钟过期，100% 预填命中；保留单个 CHM，不采用分桶。10 万设备下 Guava 强引用 / 原软引用过期 / CHM 分别为 50.34M / 5.62M / 113.52M QPS（CHM 相比两条基线分别提升 125.5% / 1920.2%）；100 万设备下分别为 19.42M / 5.79M / 38.14M QPS（分别提升 96.4% / 559.0%）。10 万设备的历史结果使用 512 MiB heap，不能与本轮绝对值混算。结果仅代表本地查找和订阅，不代表端到端上报；原始结果为 `/private/tmp/jetlinks-current-chm-{100k,1m}-confirm.json`。定向运行 `ConcurrentValidatedDeviceCacheTest`、`MonoValidatedDeviceOperatorTest`、`ClusterDeviceRegistryTest`、`EventBusStorageManagerTest` 共 34 项，0 failure、0 error。
