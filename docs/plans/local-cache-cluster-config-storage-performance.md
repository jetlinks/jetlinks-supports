# LocalCacheClusterConfigStorage 稳态读取优化

## 一级缓存观测与一致性

观测归属 `src/main/java/org/jetlinks/supports/config/`，复用 `EventBusStorageManager` 注册的标准 MBean：

`org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage`

`CacheSize` 保持缓存对象（storage）数量的既有口径。新增 `Statistics` 返回标准 JMX `CompositeData`，包含以下节点级累计量和状态：

- 规模：`storageCount`、`localEntryCount` 与计数扫描的起止时间、已访问 storage 数和完成标志；条目数包含负缓存、加载中和尚未清理的条目，首次完整扫描前为 `-1`。
- 读取：默认 `ReadSamplingEnabled=false`、`readSampleInterval=0`，此时读取样本不可用，不能把零样本解释为零命中率。`configureReadSampling(true)` 开启固定 `1/1024` 调用抽样；仅读取计数在开关切换时更换 `statisticsEpoch`，回源和失效累计量保留到 manager 结束。
- 回源：实际 L2 请求次数、Key 数、成功、失败、取消、在途、累计纳秒和六个非累计耗时桶（≤1 ms、≤5 ms、≤20 ms、≤100 ms、≤1 s、>1 s），空结果完成也属于一次成功的 L2 请求。自定义 fallback loader 不计为 Redis 请求。
- 写入与失效：L2 写成功/失败、通知发送成功/失败、远端接收/处理异常/本地未持有 storage、失效条目、单 Key 失效时发现的在途单键加载、过期清理和刷新次数；另有加载失败使用旧值与版本冲突计数。全量清空的失效条目数为并发下的近似值。
- 上下文：实例身份、快照时间、namespace、空闲 TTL、通知订阅句柄状态、最近成功回源/通知处理/错误时间及错误类型。发布成功不代表全部远端节点已处理，订阅句柄存活也不代表通知必达。

读取抽样按真实 L1 判定计正值、负值和未命中 Key；共享正在进行的加载属于未命中。批量按输入位置计量，双/三键快路径回退只计一次。命中率为 `(valueHitKeys + negativeHitKeys) / (valueHitKeys + negativeHitKeys + missKeys)`；代次变化应重新建立读取窗口。

存量扫描只汇总已有 map 大小，复用共享调度器，每秒最多处理 65536 个 storage 或协作预算 5 ms，目标每 30 秒刷新完整快照；单轮超过 120 秒则终止。getter 不触发扫描，部分结果不覆盖上次完整总量；完整扫描也是弱一致快照。manager 关闭时释放扫描迭代器、调度和自己注册的 MBean，不抢占同名注册。

`inspect(storageId,maxKeys)` 只查看已持有的本地条目状态，返回 1–128 个条目，Key 最多 512 字符并标记截断；不返回配置值，不创建 storage、不续期、不访问 L2。旧 `search` 的行为保持兼容，不能作为被动周期统计来源。TTL 仍是空闲过期，不保证热 Key 的最大陈旧时间。

失效会移除正在加载的条目，旧请求允许结束，但迟到的单键或批量正/负结果只能写入脱离当前 map 的旧对象。批量每次订阅独立持有加载状态，重试不能复用已消费的 map。本地新值仅在 L2 写入成功后可见；未订阅、取消或失败不暴露未提交值。L2 已提交但通知发布失败仍向调用方报错，并分别计入写成功和通知失败，不伪装为回滚。

实现不新增 Key 级监控字段、Span、日志或周期回源，不扩展通知 wire 格式，不提供分布式一致性承诺、诊断任务系统或前端页面。读取抽样因快路径成本默认关闭，按需启用时应重新评估业务负载。

### 本轮验证与限制

隔离副本完成 Maven 编译与 36 项定向测试（`ConfigStorageMonitorTest` 17、原存储测试 15、含真实 Redis 的管理器测试 4），零失败/跳过。覆盖未订阅/失败/取消写入、通知发布失败的部分成功、加载中单 Key/整组失效、迟到单键与批量正负结果、重试/取消、正负抽样与回退记账、被动查看不续期不回源、数量/Key 长度上限、扫描预算/超时及 MBean 注册归属和关闭。

```bash
mvn -o -Dtest=LocalCacheClusterConfigStorageTest,EventBusStorageManagerTest,ConfigStorageMonitorTest test
```

JMH 使用同一原实现基线与最终 basic 实现，2 forks，3 × 1 s 预热，4–5 × 1 s 测量及 GC profiler。1/8 线程结果分别为每线程独立缓存，不冒充共享热点争用或真实设备吞吐。

| 稳定命中路径 | 1 线程 ns/op 基线 → basic | 8 线程 ns/op 基线 → basic | B/op 基线 → basic |
| --- | ---: | ---: | ---: |
| 批量 2 键正值 | 7.379 → 7.669 | 9.616 → 10.198 | 48 → 48 |
| 批量 3 键正值 | 11.378 → 11.658 | 14.535 → 15.952 | 56 → 56 |
| 批量 8 键正值 | 68.670 → 55.592 | 77.148 → 78.766 | 416 → 416 |
| 批量 8 键混合 | 53.498 → 45.706 | 未复测 | 352 → 352 |
| 单键 `getConfig` 正值 | 3.118 → 3.158 | 3.285 → 3.801 | 0 → 0 |
| 单键 `getConfig` 负值 | 2.888 → 3.366 | 3.341 → 3.534 | 0 → 0 |

单线程开启 60 s 空闲 TTL 后，单键正值为 28.293 → 27.569 ns，负值为 28.181 → 28.055 ns，均 0 B/op。跨轮和多线程有噪声，上表不作为业务收益承诺。稳定命中分配不变，但 2/3 键等部分路径仍超过原 basic 3% 相对回退目标（例如 8 线程 3 键约 +9.7%，绝对 +1.42 ns）；不能宣称全部成本门槛通过。早期 `sampled(1/1024)` 对 2/3/8 键分别约 +22.7%/+16.2%/+31.4%，因此默认关闭读取抽样，保留一个可切换的低成本判定分支，不引入额外快路径策略层。

容量探针在 768 MiB 最大堆下创建 100 万个 storage，其中 1 万个各缓存 8 Key：计数为 100 万 storage/8 万条目，16 个有界切片完成；最长切片 3.398 ms，调用线程扫描 CPU 合计 20.877 ms。扫描直接驱动而非等待真实 1 s 调度，验证容量和单次预算，不作为周期调度长稳证据。JOL 中 monitor 可达图为 1368 → 1368 字节，storage/Cache 对象在基线和候选均为 48 字节；该结果描述监控对象与单个业务对象布局，不等于整个 manager 的 retained heap。JMX `Statistics` Java 序列化为 3131 字节。

原始本地结果：`/private/tmp/local-cache-monitor-{base-recheck,final}-t1.json`、`/private/tmp/local-cache-monitor-{base,final}-t8.json`、`/private/tmp/local-cache-monitor-{base,final}-single-ttl.json`、`/private/tmp/local-cache-monitor-capacity.log`。生产读取路径采用这些 basic 结果对应的实现；定向回归最终再次编译确认。未执行真实设备链路 CPU/input、P99 A/B、32 线程热点争用和 24 小时 churn/GC/RSS 长稳；既有缓存优化的历史 A/B 不能替代本轮成本验收。提交与 PR 尚未生成。

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
