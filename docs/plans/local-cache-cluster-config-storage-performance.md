# LocalCacheClusterConfigStorage 稳态读取优化

## 一级缓存观测与一致性

观测归属 `src/main/java/org/jetlinks/supports/config/`，复用 `EventBusStorageManager` 注册的标准 MBean：

`org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage`

性能预算优先：相对 `a160079` 无观测基线，1/8/32 线程共享命中与持续加载吞吐回退最多 5%。用户已确认移除命中抽样，将加载指标收敛为固定 1/1024 的样本统计。读取 API 和命中路径不执行观测判定、随机数、计数或观测计时；不新增 Mono/Flux 包装、观测操作符、独立 Load 对象或 Key 级监控字段。写入成功后更新 L1 的 `doOnSuccess` 负责业务可见性，继续保留。

`ConfigStorageMonitor` 只负责同步计数与快照；现有 `Cache.Loader`、`MultiCacheLoader` 在开始时随机选择 1/1024 的加载，未选中的正常加载不读取观测时钟、不写共享计数、不做终态 CAS；异常仍沿原调用链传播并记录错误上下文。选中的开始时间同时作为终态所有权，CAS 置零只结算一次，不另加终态状态字段。单键更新值会取消自身，`BaseSubscriber` 完成回调异常后也可能调用错误回调，因此样本仍须去重；无样本时零值直接跳过。默认单键读取移除空 fallback 的 `switchIfEmpty`：无 fallback 直接订阅 L2，显式 fallback 继续在 L2 空完成时订阅；命中、空完成、错误、共享读取及 fallback 语义均保持。

### 运维契约

`CacheSize` 保持 storage 数量口径。`Statistics` 为标准 JMX `CompositeData`：

- 规模：`storageCount`、`localEntryCount`、扫描起止时间、已访问 storage 数和完成标志；条目数包括负缓存、加载中和未清理条目，首次完整扫描前为 -1。
- 加载采样：`loadSampleInterval=1024`；`sampledLoadRequests`、`sampledLoadKeys`、`sampledLoadSuccesses`、`sampledLoadFailures`、`sampledLoadCancellations`、`sampledLoadsInFlight`、`sampledLoadNanos`，以及 `sampledLoadDurationBucket0..5`（非累计桶：≤1 ms、≤5 ms、≤20 ms、≤100 ms、≤1 s、>1 s，覆盖所有样本结果）。这些全部是样本原值，未乘以 1024，不能当作全量次数或全量在途数。零样本不等于零请求；估算需明确比例、样本数与窗口，不承诺小样本误差。
- 加载范围：每个现有 subscriber 选择一次，重试/重复订阅独立选择，共享加载不新增尝试。单键包括原 `switchIfEmpty` fallback 的等待、失败和取消；空完成也算成功，指标不是精确 Redis 请求数。`lastSampledLoadSuccessAt` 只表示最近一次选中样本的成功时间。
- 失效：接收、处理异常、本地未持有 storage、失效条目、单 Key 失效时发现的在途单键加载、过期清理和刷新仍记录实际事件；另有旧值兜底与版本冲突计数。全量清空的失效条目数为并发近似值。
- 上下文：`schemaVersion`、`instanceId`、`capturedAt`、namespace、空闲 TTL、通知订阅句柄状态、最近通知处理和错误时间及错误类型。重启以 `instanceId` 区分累计窗口；订阅句柄存活不代表通知必达。

本 PR 尚未发布的读取抽样字段、`ReadSamplingEnabled`、`configureReadSampling` 和全量加载字段已移除，不保留中间形态的兼容分支。写入/通知发布成功失败继续沿原调用链传播，不为计数添加操作符。前端尚未接入专用配置缓存视图；通用 MBean 工具可读取当前属性。

规模扫描只汇总已有 map 大小，复用共享调度器，每秒最多处理 65536 个 storage 或协作预算 5 ms，目标每 30 秒刷新完整快照，单轮超过 120 秒终止。getter 不启动扫描，部分结果不覆盖上次完整总量；完整扫描也是弱一致快照。关闭时释放迭代器、调度和本实例注册的 MBean，不抢占同名注册。

`inspect(storageId,maxKeys)` 只查看已持有的本地条目，最多 128 条，Key 最多 512 字符并标记截断；不返回配置值，不创建 storage、不续期、不访问 L2。两种固定 JMX schema 只保留字段名和类型，不持有运行时值、业务 Key 或 manager。旧 `search` 保持兼容，不能作为被动周期统计。TTL 仍为空闲过期，不保证热 Key 的最大陈旧时间。

### 一致性与批量状态

失效会移除加载中的条目，旧请求允许结束，但迟到的单键/批量正负结果只能写入脱离当前 map 的旧对象。本地新值仅在 L2 写入成功后可见，未订阅/失败/取消不暴露未提交值；L2 已提交但通知失败仍向调用方报错，不伪装回滚。

批量 Mono 保留只读的加载目标和可选 L1 初值，每次订阅仅创建自身结果 Map，不复制或消费目标 Map，也不为全未命中提前创建空结果种子。通过 `putIfAbsent` 合并重复判断与首次结果写入，只有缓存并发更新导致返回值变化时才覆盖；重复/未知记录忽略。显式负值使用仅在订阅内可见的标记，返回前移除，不泄露为配置值。剩余正值数避免完整正值加载后的二次遍历；重试和重复订阅始终拥有独立结果，部分失败不能污染下次订阅。

### 验证与性能预算

隔离副本 Maven 编译和 43 项定向回归通过（观测 24、存储 15、含真实 Redis 的管理器 4），零失败/错误/跳过。覆盖写入未订阅/失败/取消、通知部分成功、加载中失效及迟到正负结果、重试/取消、单键 fallback 全部结果、完成回调异常后的去重、被动查看与扫描/MBean 生命周期。新增覆盖部分结果后重试、重复和显式负值记录的订阅隔离，以及 OpenData 只暴露明确的样本口径。

```bash
mvn -o -Dtest=LocalCacheClusterConfigStorageTest,EventBusStorageManagerTest,ConfigStorageMonitorTest test
```

并发夹具为 `src/test/java/org/jetlinks/supports/config/ConfigStorageConcurrencyBenchmark.java`。同一编译夹具分别加载 `a160079` 和候选生产类；命中共享同一 storage/Key，持续加载每线程清空自身 L1、共享同一 manager。L2 为同步内存实现，测量含清空 L1，不含失效通知、扫描或 JMX 轮询，不代替 Redis 或设备消息链路。

性能预算尚未验收通过。JDK 21.0.10、512 MiB 固定堆，JMH 3 forks、3×1 s 预热、4×1 s 测量、GC profiler。下表为最终候选相对 `a160079` 的吞吐变化，负数表示回退；32 线程为超过本机 18 个逻辑 CPU 的超额订阅。

| 场景 | 1 线程 | 8 线程 | 32 线程 |
| --- | ---: | ---: | ---: |
| 单键热命中 | +3.59% | -1.26% | -6.32% |
| 8 键热命中 | -2.66% | +9.26% | -0.23% |
| 单键持续加载 | +14.23% | +7.43% | -13.38% |
| 8 键持续加载 | +1.53% | +14.52% | +2.96% |

单键热命中零分配；单键加载约 448→392 B/op，8 键加载约 2696→2712 B/op。8 键命中路径未变化，复用同一夹具的有效切片 `/private/tmp/config-five-final-{baseline,optimized}-t{1,8,32}.json`；其余为 `/private/tmp/config-five-put-{baseline,optimized}-t{1,8,32}.json`，没有删除超预算结果。

32 线程固定同一候选重复运行时，单键命中从约 44.68 亿变化到 56.96 亿次/秒，相差约 27%；不能把该宿主的整段顺序测试直接解释为确定的实现成本。进一步预先固定四组交替顺序、逐场景运行的独立 JVM 对照（每组每版本 1 fork，4×1 s 预热、5×1 s 测量）：单键命中各组 -1.20%、-1.77%、+2.05%、+0.81%，汇总 -0.09%；单键加载各组 -8.23%、-0.49%、+34.71%、+10.55%，汇总 +7.62%。结果为 `/private/tmp/config-five-final32-{singleHit,singleReload}-{baseline,optimized}-{1,2,3,4}.json`。加载仍有超预算组和较大方差，不能以均值改善或置信区间重叠宣称严格达标；需在隔离环境继续验收，目标仍为最多 5% 回退。

生产代码指纹（SHA-256 前 16 位）：`ConfigStorageMonitor=a3acd43e641ecb35`、`EventBusStorageManager=08e22bab3ea6fb71`、`LocalCacheClusterConfigStorage=7b453a4f24bcdf90`。无稳定收益的终态字段与热命中分支实验已撤回，测试和最终基准使用上述同一实现。

补充 32 线程边界（2 forks，其余预热、测量及堆参数同主矩阵）：逐线程 fixture 的 1 键批量负缓存、单键负缓存、双键正值、三键正值吞吐分别 +13.34%、+2.77%、+6.67%、+2.32%；共享热点启用 TTL=60000 ms 后，单键与 8 键命中分别 -0.75%、-1.33%。数据为 `/private/tmp/config-five-put-{fast,ttl}-{baseline,optimized}-t32.json`，未出现超过 5% 的吞吐回退。这些结果不覆盖失效风暴或真实 L2 延迟。

相同 32 线程、2 forks 的 JMH sample-time 诊断中，单键加载 P99 为 0.792→0.833 μs（+5.18%），8 键加载为 2.416→2.456 μs（+1.66%）；数据为 `/private/tmp/config-five-put-sample-{baseline,optimized}-t32.json`。单键尾延迟点估计仍超过预算，不能四舍五入为达标；该样本是同步内存 L2 的方法延迟，不是 Redis 或真实设备链路 P99。

复测：先分别编译基线和候选生产类，再仅使用候选编译的 `target/test-classes` 和同一依赖 classpath。令 `cache_runtime` 指向被测版本、`cache_fixture` 指向候选、`cache_benchmark_cp` 为依赖 classpath，`cache_threads` 依次为 1、8、32：

```bash
java -cp "$cache_fixture/target/test-classes:$cache_runtime/target/classes:$cache_benchmark_cp" \
  org.openjdk.jmh.Main 'ConfigStorageConcurrencyBenchmark.(singleHit|batchHit8|singleReload|batchReload8)' \
  -p monitoring=basic -p ttlMillis=-1 -t "$cache_threads" \
  -f 3 -wi 3 -i 4 -w 1s -r 1s -bm thrpt -tu s \
  -jvmArgs '-Xms512m -Xmx512m' -prof gc -rf json -rff "$cache_output.json"
```

对不稳定场景按上述交替顺序复核，不并发运行其他 CPU 密集任务；隔离环境仍须覆盖负缓存、双/三键命中、TTL、实际 Redis 和真实设备消息负载，不以同步内存基准代替端到端性能上限。

扫描算法未变，复用容量证据：768 MiB 最大堆下创建 100 万 storage、8 万条目，16 个切片完成，最长 3.398 ms，调用线程扫描 CPU 合计 20.877 ms。直接驱动扫描不等于真实周期调度长稳。真实设备链路 CPU/input、P99 A/B 和 24 小时 churn/GC/RSS 尚未执行；历史缓存优化数据不能替代这些未覆盖项。交付 PR：[jetlinks-supports #48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

## 双、三键全 L1 命中复核

目标：在已完成的通用 L1 快路径基础上，隔离验证设备常用的双、三键读取是否值得专门构造 `Values`。仅在全部键已有缓存值时进入快路径；缺键、失效、重复键和负缓存必须维持原有语义，不调整缓存加载、失效或版本逻辑。使用相同 JMH 夹具比较原实现和候选的 1/8 线程吞吐及分配，辅以定向回归。若完整链路没有明确增益，不引入新的常驻状态或操作符。

同一 `bede1d8` 基线的两个隔离工作树，JDK 21.0.10、JMH 2 forks、2 × 1 s 预热、3 × 1 s 测量、GC profiler；原始结果为 `/private/tmp/supports-config-{base,candidate}-t{1,8}.json`。候选生产文件及语义测试与已有主工作区未提交改动逐字相同，主工作区没有参与构建。

| 热 L1 键数 | 1 线程 ns/op 基线 → 候选 | 8 线程 ns/op 基线 → 候选 | B/op 基线 → 候选 |
| --- | ---: | ---: | ---: |
| 2 | 21.40 → 7.81 | 27.42 → 10.48 | 176 → 48 |
| 3 | 27.91 → 11.22 | 37.70 → 16.39 | 208 → 56 |
| 8（对照） | 75.15 → 67.64 | 80.93 → 87.80 | 416 → 416 |

双、三键单独读取得到稳定且明显的分配与耗时收益；8 键分配不变，耗时置信区间重叠，不能判定提升或回退。定向 `LocalCacheClusterConfigStorageTest` 15 项与 `EventBusStorageManagerTest` 4 项均通过；沙箱内 Mockito agent 无法自附加，测试在允许 JVM attach 的环境中运行通过。

设备消息 `DeviceMessageConnector.refactorHeader` 读取 8 个自身配置键和 1 个继承键，**不会走双、三键快路径**。因此不能把此微基准收益写成设备上报全链路收益，也不为该候选额外跑无针对性的完整链路。此前通用 L1 快路径已在 #42 实现并完成生产 Storage A/B，见下节；若以设备上报 QPS 为目标，当前双、三键候选应作为独立配置读取优化评估，而不是这条链路的下一步性能改造。

## 目标与范围

- owning module：`jetlinks-supports`
- 优化入口：`src/main/java/org/jetlinks/supports/config/LocalCacheClusterConfigStorage.java`
- 目标：降低 `getConfigs` 一级缓存命中路径的 CPU 与瞬时分配，不修改 Core SPI、
  Redis 二级缓存协议、集群失效通知和缓存版本语义。
- 不引入跨消息 Header/配置快照缓存，也不使用额外 Reactor 操作符包装同步读取。

## 实现

1. `getOrCreateCache` 先执行无分配的 `Map.get`，仅在首次创建时进入
   `computeIfAbsent`。
2. `getConfigs` 延迟创建结果 Map，只保存非 null 配置，直接构造 `Values`，移除
   `Maps.filterValues` 视图及其迭代器。
3. 全负缓存或空结果复用不可变 `Values` 与 `Mono`。
4. 二级缓存未命中完成时同样复用空 `Values`；正、负、混合命中仍写入原有
   `Cache`，后续读取不重复访问 L2。

负缓存继续表现为“配置不存在”，不会通过 `Values.getValue` 或
`Values.getAllValues` 暴露 null 值。并发加载、版本比较和
`CacheNotify` 集群失效路径未修改。

## 验证

基准提交为 `3ead6e2`。JMH 使用 JDK 21、2 forks、3 次 1 秒预热、5 次 1 秒测量和
GC profiler；结果文件由以下测试基准生成：

`src/test/java/org/jetlinks/supports/config/LocalCacheClusterConfigStorageBenchmark.java`

| 场景 | 耗时（ns/op）基准 → 优化 | 耗时变化 | 分配（B/op）基准 → 优化 | 分配变化 |
| --- | ---: | ---: | ---: | ---: |
| 8 个正缓存命中 | 200.831 → 142.605 | -28.99% | 656.026 → 416.020 | -36.58% |
| 8 个正负混合命中 | 203.033 → 120.901 | -40.45% | 656.028 → 352.017 | -46.34% |
| 1 个负缓存命中 | 44.049 → 6.059 | -86.24% | 264.009 → 0 | -100% |
| 7 个 L1 命中、1 个 L2 加载 | 6870.716 → 6752.521 | -1.72% | 3608.189 → 3456.186 | -4.21% |
| 8 个 L2 加载 | 7537.341 → 7590.114 | +0.70%（噪声） | 5128.205 → 5080.228 | -0.94% |

Components 设备消息真实链路使用同一代码、数据集和 classpath，仅替换从
`3ead6e2` 构建的 Supports jar 与优化 jar。环境为 4C8G、8 个入口 worker、4 个
Reactor worker、14,000 msg/s、真实 `EventBusStorageManager -> Redis`，设备链路
包含 5000 个组织、200 个用户、每用户 500 个组织和 10000 个设备。

- `route-only`：3 组交替 A/B，每组 30 秒预热、60 秒测量。配对中位数 CPU
  下降 1.55%；分配下降 18.20%。三组结果均为 840,000 输入、3,360,000 投递，
  queue `0 -> 0`，零 drop/error/重复/乱序，checksum 一致。
- `full-platform`：120 秒预热、60 秒测量的补充 A/B。CPU
  `223,667 -> 221,488 ns/input`（-0.97%），分配
  `15,853 -> 13,808 B/input`（-12.90%），processing p50
  `0.289 -> 0.260 ms`；840,000 输入、18,480,000 投递和 1,680,000 durable
  writes 全部正确，queue `0 -> 0`，三个 buffer 完全排空。单组 CPU/延迟仅作为
  完整链路佐证，稳定收益以分配和 route-only 三组 A/B 为主。

定向测试：

```bash
mvn -o -Dtest=LocalCacheClusterConfigStorageTest,EventBusStorageManagerTest test
```

共 15 个测试通过，覆盖正缓存、负缓存、正负混合、部分失效、Redis 与集群通知。
全量 `mvn -o test` 运行到 38 个测试时仅
`DetailErrorMapperTest.testRoundTripConversion` 失败；在未修改的 `3ead6e2`
独立 worktree 中可稳定复现，确认与本次缓存优化无关。
