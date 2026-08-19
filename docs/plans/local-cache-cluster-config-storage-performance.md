# LocalCacheClusterConfigStorage 稳态读取优化

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
