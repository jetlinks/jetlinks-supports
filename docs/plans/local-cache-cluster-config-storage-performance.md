# LocalCacheClusterConfigStorage 稳态读取优化

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
