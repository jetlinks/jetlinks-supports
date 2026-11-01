# LocalCacheClusterConfigStorage 稳态读取优化

## 共享加载与一致性

终态去重与剩余计数由 LoadGroup 的同一临界区维护，不再逐 Pending 额外加锁。保留在途登记、结果订阅先于回源、lease 与 doFinally、最后读者取消、写入提前完成、重试和关闭兜底。Pending 延迟创建等待 Sink 时的同步仍然保留。回调复用、成功后的 lease 快路径及冷注册替换不采用；高并发对照没有证明这些变体比单独合并计数更可靠。整体 5% 上限继续作为未完成的验收边界。

单键直接通过 Pending 交付值/空结果，不创建批量 ReadPlan 或 NULL 过滤链。批量拥有者只等待一次整批完成；加入已有请求时才创建对应 Key 的 Sink。逐 Key 所有权、版本、共享、取消和关闭语义保留，manager 在途登记、命中路径、fallback 与底层超时边界保持原样。

本次性能收敛：用固定 64 个并发 Set 分散 manager 在途登记的共享竞争，按 Storage id 散列选择分组，沿用 ConcurrentHashMap.newKeySet 的登记/移除和关闭竞争处理。不保留每 Storage 的登记表或 id，已脱离 L1 的请求仍可取消，不依赖缓存容器淘汰回调。没有新增集合锁、关闭快照数组、超时、采样、线程池或加载抽象，命中路径保持原样。生命周期、首次加载/清理、同步 L2 与真实 Redis 的验证结果见下节。

仅共享 L2；显式 fallback 属于自己的调用方，普通读取不等另一调用方的 fallback，其他 loader 的空结果/错误也不会跳过或失败自己的 loader。保留成功结果按版本写入 L1、写入完成等待者的行为。成功发布的配置可结束其他仍在等待的 fallback；空结果和错误只结束自己的调用。没有新增 Storage 超时、观测或前端。

owning module 为 `jetlinks-supports`：`LocalCacheClusterConfigStorage` 负责每 Key 状态与共享加载，`EventBusStorageManager` 负责实例注册和关闭，`RedisClusterCache` 提供原子的读取删除。

每 Key 的版本、值和复用的命中 Mono 通过一个不可变 `Snapshot` 发布。纯值共享在锁外执行；随后在 Key 的短临界区检查加载所有权、发布新状态并准备等待者终态，锁外交付结果。旧请求不能覆盖较新写入；失效移除条目，迟到结果不能回填替代条目。日志不打印缓存值，旧 search 仅保存有界的异常类型字符串。

API 的命中探测只读取已有条目；miss 的 Cache 注册和加载对象创建均发生在实际订阅时，丢弃未订阅 Mono 不留下空条目。按 Key 认领或加入 `Pending`，新认领 Key 合并成一次 L2 请求。重叠批量和单键共享已有 Key，仅对新增 Key 回源；同一批量重复订阅没有共享的可变结果 Map。批量在已有 Pending 中暂存首次收到的值，完整成功后统一发布，不再创建第二张中间 Map；未知/重复 L2 记录不扩张结果，部分失败不污染重试。命中路径复用 Mono 或直接构造 `Values`，不创建加载对象。

边界：TTL 保持空闲过期，不增加容量阈值或专用前端；显式 fallback 在调用方订阅后执行，负缓存命中也可触发，保留既有成功结果缓存及写入可结束等待者的语义。原有 Error/容量 MBean 复用；不新增逐 Key 追踪。共享请求使用首个订阅者 Context，加入者不重复执行远端请求；最后一个使用者取消才释放共享上游。加载期限由底层 L2 或调用方控制，Storage 不新增超时配置或定时器；底层超时按普通错误处理，先释放所有权再交付错误/旧值并允许重试。上游尚未终止且读者未取消时继续等待，不属于 Storage 自身泄漏。

每个读者对每个共享请求只持有一次引用，同一批量的多个 Key 不重复 retain/release；最后一个读者取消才取消上游。共享组在暴露 Pending 前登记管理器，通过真实上游 Subscription 取消收集和执行阶段，不再构造 stop sink 或内层竞速。关闭先结束 Pending，再取消上游；关闭后才到达的 onSubscribe 会立即取消，关闭不依赖原批量线程恢复或 seal。终态先清理加载槽再交付，重入 retry 可认领新请求；写入可以完成旧等待者，管理器关闭也会结束已被失效移除的 L2/fallback。在成功或错误交付之前保留关闭登记，避免已准备但未交付的结果脱离管理。

显式 fallback 使用调用方自己的 `FallbackLoad`，独立传播 Context 和取消；仅在负结果代次未变化时提交到 L1，较新写入已准备的终态优先。共享读取在 L2 失败后选用旧负结果时，fallback 接受这个结果，不因它已过期再次回源形成内部重试循环；失效替换出的空代次仍重新读取 L2。取消、空结果和错误均移除该读者的等待记录；最后一条记录结束后释放集合。普通加载不创建这个对象或集合。清空/清理移除空 Storage 后，保留句柄重新加入管理器当前实例；旧句柄的写入和刷新同样作用于当前实例。

冷 Key 创建与空 Storage 移除使用同一个 Storage 本地锁。已有 Storage 增加新 Key 只修改自己的缓存，不执行逐 Key 的 manager.cache.compute；清理/清空后的保留句柄仍按当前注册实例重入。命中路径不获取这个锁。

单键 Pending、批量 ReadPlan 均先安装结果订阅，再启动对应 LoadGroup；每个 group 只驱动一次上游并传播首个驱动者 Context。已有 LoadGroup 作为 Reactor BaseSubscriber 直接接收单键或批量源，不构造 execution.cache、结果竞速、重复结果订阅、ready Sink 或 doOnNext/then 批量中转。每个读者对每个 group 只持有一次 lease；结果 Sink 负责背压与写入提前完成，封口前只保存 Context。单键按值/empty 交付；批量拥有者等待一次组信号，仅对别的 group 所有的 Key 建立等待信号，按每订阅私有的目标数组构造 Values。Key 终态只计入整批完成一次；关闭/错误还会检查已保留的终态，避免交付线程暂停导致整批永久等待。延迟创建的 Sink 会重放已准备的结果，不丢失完成信号。上游订阅、取消和迟到 onSubscribe 使用 BaseSubscriber 的标准实现，没有新增超时、观测或独立协调器。

`RedisClusterCache.getAndRemove` 使用单 hash-slot Lua 执行 HGET/HDEL，避免两条命令之间的并发写入被误删；参数和结果分别使用 hash key/hash value 序列化器，兼容其与顶层 value 序列化器不同的情况。

### 终态计数合并验证与性能边界

编译及 93 项定向回归通过，零失败/错误/跳过。新增回归在批量 L2 已完成、一个加入者仍暂停于收到值的回调时关闭 manager，确认整批不依赖该交付线程恢复即可结束，且在途登记清空。命令与下节相同，日志为 `/private/tmp/config-lifecycle-tight-final-validation.log`，生产源 SHA-256 前 16 位为 `ae90bcaf807d901f`。仅 Pending、LoadGroup 的归一化 javap 字节码变化；外层命中 API、Cache、Snapshot、ReadPlan 和 manager 均与 130564d 一致，复用既有命中路径证据。

D=`d41e901`，B=`130564d`，N 为终态计数合并。沿用同一编译夹具、JDK 21、JMH 1.35、1 fork、3×1s 预热、4×1s 测量、512 MiB 堆和 GC profiler，每 worker 独立 Storage/L1。1 线程按 D/B/N/N/B/D；32 线程复用拆分筛选中的 D/B/A/S/L/L/S/A/B/D，A 即最终 N，S/L 为未采用的 lease/回调变体。最终 Maven 编译的主要类及全部加载对象与筛选 A 的归一化字节码相同，不为交付重复压测。每格是两个进程样本的等权均值，阶段分别保留，不与旧轮次混算。

| 同步 L2 持续回源 | D → B → N，ns/op | N 相对 B | 分配 B → N，B/op |
| --- | ---: | ---: | ---: |
| 1 线程单键 | 88.216 → 166.504 → 166.160 | -0.21% | 632.1 → 632.1 |
| 1 线程 8 键 | 428.469 → 822.727 → 785.427 | -4.53% | 4296.3 → 4208.3 |
| 32 线程单键 | 262.589 → 670.028 → 556.599 | -16.93% | 639.0 → 620.1 |
| 32 线程 8 键 | 1461.860 → 3233.948 → 3072.502 | -4.99% | 4206.6 → 4206.6 |

1 线程单键样本交叉，不认定提升。1 线程批量 B=822.129/823.326、N=782.502/788.352 ns；32 线程单键 B=632.271/707.785、N=566.434/546.764 ns。32 线程批量 B=3487.708/2980.188、N=3185.227/2959.777 ns，样本交叉，不能宣称稳定提速。B/op 随 JVM 逃逸分析变化，不把差值当作删除了固定大小对象。

首个组合候选同时包含回调复用与成功后的 lease 快路径，32 线程单键/批量分别回退约 12.83%/12.32%，已撤销；拆分后只保留最小计数合并。原始数据分别为 `/private/tmp/config-lifecycle-tight-results/cpu-*`、`/private/tmp/config-lifecycle-tight-variants/t32-*`；最终单线程为 `/private/tmp/config-lifecycle-tight-results/accepted-cpu-*`。复现脚本为 `/private/tmp/config-lifecycle-tight-benchmark.py` 和 `/private/tmp/config-lifecycle-tight-variants/run.py`。

异步共享按 B/N/N/B、8 个 worker，每轮 8 个读者加入同一未完成的 L2，由调度线程释放，每轮断言只回源一次且结果全部正确。单键整轮 B→N 为 26545.902→26271.247 ns（-1.03%），分配 3520.3→3328.2 B；8 键整轮为 31436.969→31419.926 ns（-0.05%），分配 21376.1→21376.3 B。耗时样本交叉，按基本持平报告，不作为单读者延迟或生产 P99 收益。原始数据为 `/private/tmp/config-lifecycle-tight-results/accepted-async-*`。

真实 Redis 在 32 线程按 B/N/N/B、各 benchmark 独立 fork 运行，保留首轮负向结果：

| Redis 首轮 | B → N，µs/op | 变化 | B 样本 | N 样本 |
| --- | ---: | ---: | --- | --- |
| 直接 L2 单键 | 326.747 → 377.174 | +15.43% | 335.244/318.250 | 415.411/338.936 |
| Storage 单键 | 327.187 → 339.114 | +3.65% | 335.196/319.179 | 338.746/339.481 |
| Storage 8 键 | 398.434 → 473.267 | +18.78% | 391.406/405.463 | 494.327/452.207 |

为区分固定协调成本与独立 fork/服务波动，扩展 `ConfigStorageRedisPairBenchmark.pairedBatch8`：同一客户端、JVM 和 32 个 worker 内每轮各执行一次直接 8 键读取与冷 Storage 8 键读取，并轮换先后次序。两版本共用同一编译夹具和 CompilerHints，仅切生产类；累计耗时比 Storage/direct 为 B=1.010415/0.999695、N=1.010778/0.995630，整对耗时 B=781.480/760.964 µs、N=787.573/763.756 µs，未重现约 19% 的额外成本。该配对夹具通过 test-compile，生产 config 类目录字节码摘要保持 `3e27084c94b0b07bcd1b0cb7932fed408254566faf5cef3e46653c237a3230df`，复用 93 项回归；日志为 `/private/tmp/config-lifecycle-tight-paired-compile.log`。

配对改变了负载形态，因此再按 B/N/N/B 只复核原始纯 Storage 8 键场景。B=393.014/392.856 µs、N=374.243/408.761 µs，均值 392.935→391.502 µs（-0.36%），样本交叉，按持平报告。未重现首轮 18.78% 回退，但具体干扰源未定位，不能把首轮负向全部定性为本地服务，也不把首轮、配对和复核合并成性能通过结论。原始数据为 `/private/tmp/config-lifecycle-tight-results/accepted-redis-*`、`accepted-paired-batch-*` 和 `accepted-redis-recheck-*`。

此前 Redis 单键负向证据仍保留于下节，不因本轮 CPU 或复核结果删除。**相对原始 D 的整体 5% 上限仍未通过**：N 单键约为 D 的 1.88 倍（1 线程）/2.12 倍（32 线程），批量也明显高于 D；Redis 不宣称改善，生产 P99 与长稳仍未验证。交付沿用 [jetlinks-supports #48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

### 结果交付收敛验证与性能边界

编译及 92 项定向回归通过，零失败/错误/跳过。在既有 88 项基础上，新增封口前写入完成整批且不启动 L2、部分写入后源失败及重试、整批完成后仍遵守下游需求、16 线程同读 8 Key 时逐 Key 只回源一次且所有读者结束；同时扩展只加入单个 Key 的批量可由写入提前完成。最终源指纹为 `LocalCacheClusterConfigStorage=732fb128c0d22427`，manager 指纹 `792fb7cb32cad156` 未变。日志为 `/private/tmp/config-result-converge-final-validation.log`，定向命令同下节。

API/命中探测、TTL、Cache 的命中读取和 Snapshot 经 javap 归一化核对，与 3d8b717 字节码一致；复用其命中路径证据。新增异步共享与同进程 Redis 交错计时的 JMH 夹具已通过 test-compile，生产 config 类字节码摘要保持不变，复用上述 92 项回归。

D=`d41e901` 为共享加载重构前，C=`3d8b717` 为本次修改前，N 为结果交付收敛候选。最终同轮对照使用相同编译夹具、JDK 21、JMH 1.35、1 fork、3×1s 预热、4×1s 测量、512 MiB 固定堆和 GC profiler。同步 L2 在 1/32 线程各按 D/C/N/N/C/D 顺序，每 worker 独立 Storage/L1；其余对照按 C/N/N/C。无并行构建或本任务的并行压测，保留两个进程样本等权均值：

| 同步 L2 持续回源 | D → C → N，ns/op | N 相对 C | 分配 C → N，B/op |
| --- | ---: | ---: | ---: |
| 1 线程单键 | 89.178 → 183.106 → 167.323 | -8.62% | 716.1 → 632.1 |
| 1 线程 8 键 | 426.760 → 1146.140 → 801.419 | -30.08% | 6040.5 → 4208.3 |
| 32 线程单键 | 256.885 → 690.642 → 666.101 | -3.55% | 695.0 → 630.6 |
| 32 线程 8 键 | 1287.442 → 4190.252 → 3344.909 | -20.17% | 6051.2 → 4206.9 |

32 线程单键 C=726.872/654.413 ns，N=661.652/670.550 ns，样本交叉，不认定稳定改善；批量 C=4149.142/4231.362 ns，N=3554.276/3135.541 ns。单键分配随 JVM 逃逸分析略有变化，表中为实际均值，不将差值当作恒定对象大小。批量分配约减少 30%。

异步共享夹具使用 8 个 JMH worker，每个 worker 的一次操作先让 8 个读者加入同一未完成的 L2，再由调度线程释放；每轮断言只有一次 L2 请求且全部结果正确。单位为整轮 8 读者的 ns/op：

| 异步共享 | C → N，ns/轮 | N 相对 C | 分配 C → N，B/轮 |
| --- | ---: | ---: | ---: |
| 单键 | 26251.376 → 26131.023 | -0.46% | 3992.4 → 3520.2 |
| 8 键批量 | 31369.645 → 30001.785 | -4.36% | 22286.7 → 21394.1 |

32 线程真实 Redis 的各 benchmark 在独立 fork 中运行；直接 L2 不经过 Storage，不能逐操作配对或用延迟相减：

| Redis | C → N，µs/op | N 相对 C | C 样本 | N 样本 |
| --- | ---: | ---: | ---: | ---: |
| 直接 L2 单键 | 349.418 → 314.271 | -10.06% | 346.446/352.390 | 310.952/317.590 |
| Storage 单键 | 338.945 → 405.142 | +19.53% | 326.365/351.525 | 433.748/376.536 |
| Storage 8 键 | 512.263 → 384.993 | -24.84% | 491.292/533.234 | 397.901/372.084 |

保留单键负向结果。为区分固定路径开销与独立 fork/服务波动，另在同一客户端、JVM 和 32 个 worker 中交替计时直接 L2 与 Storage，并轮换先后次序。每对操作都包含一次直接读取和一次冷 Storage 读取；AuxCounters 汇总各自耗时，使用 Storage/direct 累计耗时比。C 两次为 1.008847/1.001983，N 为 0.999051/0.997553；整对耗时 C=671.371/663.327 µs，N=697.814/636.262 µs。这组对照未复现 19.53% 的 Storage 额外等待，但交错读取改变了并发负载形态，不能排除纯 Storage 高并发场景的差异，也未定位前组具体干扰源。两组分别保留，不合并成性能通过结论。

**相对原始 D 的整体 5% 上限仍未通过，Redis 单键不宣称改善。** 同步单键 N 仍为 D 的约 1.88 倍（1 线程）和 2.59 倍（32 线程），批量也高于 D。尚未验证生产端到端 P99 和长稳。

原始结果：`/private/tmp/config-result-converge-controlled-cpu-t{1,32}-{1-D,2-C,3-N,4-N,5-C,6-D}.json`、`config-result-converge-async-t8-{1-C,2-N,3-N,4-C}.json`、`config-result-converge-redis-t32-{1-C,2-N,3-N,4-C}.json`、`config-result-converge-redis-paired-{1-C,2-N,3-N,4-C}.json` 及同名日志；运行脚本为 `/private/tmp/config-result-converge-benchmark.py`。CPU/异步/Redis 统一使用 CompilerHints 摘要 `6fc0142e35e2d472`，包含原 CPU/Redis 与新增异步方法提示。早期 `config-result-converge-cpu-*` 是中间代码；`config-result-converge-final-cpu-*` 因新增资源遮蔽旧 CompilerHints、序列中途停止，不参加最终验收，原始文件保留。

### 3d8b717 的验证与性能边界（结果交付收敛前）

编译及 88 项定向回归通过，零失败/错误/跳过。在既有 86 项基础上，新增订阅回调中立即取消不启动 L2、加入尚未封口批量的读者以自己的 Context 驱动一次 L2 的验证。继续覆盖背压、原子发布、重入重试、最后一个读者取消、写入提前完成、失效后关闭、收集期关闭、迟到 onSubscribe、独立 fallback 和真实 Redis 序列化。日志为 `/private/tmp/config-single-driver-final-tests.log`。

定向命令：`mvn -o -Dtest=ConfigStorageFollowupReviewTest,LocalCacheClusterConfigStorageTest,ConfigStorageMonitorTest,LocalCacheClusterConfigStorageLifecycleTest,EventBusStorageManagerTest,RedisClusterCacheTest -Dsurefire.forkedProcessTimeoutInSeconds=60 test`。生产指纹 SHA-256 前 16 位：`LocalCacheClusterConfigStorage=8acb14db7b0b2946`、`EventBusStorageManager=792fb7cb32cad156`。L1 命中 API、查找、TTL、值读取和 Snapshot 方法经 javap 归一化对照，与 f172221 字节码一致，复用既有命中路径证据。

根因：原实现把一次加载拆成执行 Mono 和结果 Sink 两路，再用 execution.cache、firstWithSignal、then(result) 协调。JFR 的 32 线程单键样本可见 RaceCoordinator、FirstEmittingSubscriber、MonoCacheTime、MonoIgnoreThen 以及相应订阅/分配；这些不是观测工作。当前直接复用已有 ReadPlan/LoadGroup 作为 Publisher/Subscriber 边界，删除缓存执行链、ready Sink、结果竞速和重复结果订阅；单键直接接收 L2 值及空完成，不再经 doOnSuccess/then 转成 Void。结果 Sink 仍处理背压和逐 Key 提前完成，上游订阅/迟到取消由 Reactor BaseSubscriber 处理，没有手写响应式协议或增加新的生产协调类。候选 JFR 中未再采到这些竞速/执行缓存类的分配，剩余 CPU 样本主要是 map 读写、setOnce、Cache.complete 和夹具的清空 L1/block；JFR 为诊断样本，不与无录制的性能均值合并。录制为 `/private/tmp/config-single-f172221.jfr`、`/private/tmp/config-single-driver-final.jfr`。

D=`d41e901` 为共享加载前原始基准，B=`f172221` 为本次修改前，C 为当前实现。同一编译夹具、JDK 21、JMH 1.35、1 fork、4×1s 预热、5×1s 测量、512 MiB 固定堆及 GC profiler。1/32 线程同步 L2 各按 D/B/C/C/B/D，每个 worker 使用独立 Storage id/L1，无并行构建或其他本任务压测。各版本两个样本均值：

| 同步 L2 持续回源 | D → B → C，ns/op | C 相对 B | C 相对 D | 分配 D → B → C，B/op |
| --- | ---: | ---: | ---: | ---: |
| 1 线程单键 | 88.831 → 368.596 → 174.598 | -52.63% | +96.55% | 384.0 → 1552.1 → 672.1 |
| 1 线程 8 键 | 425.084 → 1429.695 → 1122.364 | -21.50% | +164.03% | 2704.2 → 6944.6 → 6040.5 |
| 32 线程单键 | 246.108 → 1130.931 → 663.396 | -41.34% | +169.56% | 384.0 → 1551.6 → 671.2 |
| 32 线程 8 键 | 1278.194 → 5120.945 → 4331.876 | -15.41% | +238.91% | 2703.2 → 6936.1 → 6063.3 |

32 线程单键 B 为 1143.633/1118.229 ns，C 为 621.391/705.400 ns；批量 B 为 5243.664/4998.225 ns，C 为 4477.469/4186.283 ns，各候选样本均快于各修改前样本。1 线程单键减少 880 B/op（56.70%），批量减少 904 B/op（13.02%），删除了实际协调分配；分配变化和耗时变化分别报告，不能用前一轮相对基准的改善替代本轮同轮结果。原始文件为 `/private/tmp/config-single-driver-cpu-t{1,32}-{1-D,2-B,3-C,4-C,5-B,6-D}.json`。

真实 Redis 5 使用同一参数按 D/B/C/C/B/D 运行，各版本两个样本均值：

| 32 线程真实 Redis | D → B → C，µs/op | C 相对 B | C 相对 D | 分配 D → B → C，B/op |
| --- | ---: | ---: | ---: | ---: |
| 单键 | 322.955 → 324.661 → 396.609 | +22.16% | +22.81% | 14444.0 → 15631.3 → 14615.0 |
| 8 键 | 368.624 → 481.203 → 384.886 | -20.02% | +4.41% | 57574.6 → 61887.5 → 60986.8 |

保留负向结果：单键 B 为 318.086/331.237 µs，C 为 339.027/454.192 µs；批量 B 为 385.334/577.072 µs，C 为 381.411/388.361 µs。不能用同步 L2 的收益证明 Redis 全面改善；原始文件为 `/private/tmp/config-single-driver-redis-{1-D,2-B,3-C,4-C,5-B,6-D}.json`。

为区分 Storage 与 L2/进程波动，新增 directSingle 直接调用同一个 RedisClusterCache.get().block()，不经过 Storage；仅编译夹具，核对生产 config 类字节码未变化，复用 88 项回归。按 B/C/C/B 运行 directSingle 和 singleReload，其他参数相同，分别在各自 fork 中测量，结果不能作逐操作配对或直接相减：

| 32 线程复核 | B → C，µs/op | 变化 | B 样本 | C 样本 |
| --- | ---: | ---: | ---: | ---: |
| 直接 L2 | 311.207 → 392.615 | +26.16% | 305.669/316.744 | 376.317/408.913 |
| 经过 Storage | 347.006 → 332.024 | -4.32% | 360.819/333.194 | 327.945/336.104 |

直接 L2 的路径没有执行本次变更的加载代码，也出现 26.16% 进程间差异，而经过 Storage 的对照方向反转。这支持环境/JVM/远端链路波动足以影响当前样本，但未捕获首轮具体干扰源，不能将首轮 +22.16% 全部定性为本地服务造成。首轮负向数据不删除、不与复核混合成通过结论；Redis 严格 5% 上限继续未通过。原始文件为 `/private/tmp/config-single-driver-redis-control-{1-B,2-C,3-C,4-B}.json`，相关 Redis/manager 生产类及直接读取夹具在两个运行时逐字节相同。

**原始基准的整体 5% 目标仍未通过**：当前同步单键仍为原始 D 的约 1.97 倍（1 线程）、2.70 倍（32 线程），批量也明显高于 D，不能将这部分差距写成已解决或全部归于本机波动。D 是提前订阅 L2 的旧实现，未提供当前的懒执行/Context 和完整共享取消/关闭保证；当前还保留加载认领、lease、在途登记、值/版本原子发布及逐 Key 结果协调，未为追平旧实现而撤销这些行为。设备消息端到端 P99 和 24 小时长稳尚未验证，交付继续使用 [#48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

固定 64 个登记分组和 manager 生产代码保持 f172221 原样。该结构在顺序淘汰 10000 个 Storage 后 JOL 估算约 8 KB，在途集合为空；并发峰值仍可扩张分组内部容量/计数结构。复用原登记阶段证据 `/private/tmp/config-load-partitions-tests.log`、`/private/tmp/config-load-groups-partitions-{cpu-t{1,32},churn-t{1,32}}-{1-B,2-T,3-T,4-B}.json`、`/private/tmp/config-load-groups-partitions-redis-{1-D,2-T,3-T,4-D}.json`；其时延不能替代本次新的订阅链实测。

未采用方案保留的反例：按 id 保留空登记会增加约 126.6 B/Storage，且不能跟随自定义容器独立淘汰；固定 HashSet 显式锁在单线程批量持续回源对照中回退约 8.4%（`/private/tmp/config-load-groups-stripes-single-{1-B,2-S,3-S,4-B}.json`）。没有采用这些方案或调整分组数量反复寻优。

### b3ac16c 的验证与性能边界（固定分组前）

编译及 84 项定向回归通过，零失败/错误/跳过：原有 82 项，加清理与冷 Key 注册竞争、关闭后才到达上游订阅的两项回归。取消直接传播到真实上游；清理后读者仍注册于管理器并能接收失效通知。日志为 `/private/tmp/config-converge-stage2.log`。生产代码只调整两个已有类，没有新增协调器、配置、后台任务或观测。

定向命令：`mvn -o -Dtest=ConfigStorageFollowupReviewTest,LocalCacheClusterConfigStorageTest,ConfigStorageMonitorTest,LocalCacheClusterConfigStorageLifecycleTest,EventBusStorageManagerTest,RedisClusterCacheTest -Dsurefire.forkedProcessTimeoutInSeconds=60 test`。

对照 B=`cad170f` 与收敛候选 C：同一编译夹具、JDK 21、1 fork、4×1s 预热、5×1s 测量、512 MiB 固定堆和 GC profiler。1 线程同步 L2 顺序 B/C；32 线程同步 L2 与真实 Redis 5 均使用 B/C/C/B，每个 worker 使用不同 Storage id 和独立 L1，无并行构建或其他本任务压测。

| 场景 | 耗时 B → C | 耗时变化 | 分配 B/op B → C |
| --- | ---: | ---: | ---: |
| 1 线程，同步 L2 单键回源 | 377.386 → 366.050 ns | -3.00% | 1776 → 1552 |
| 1 线程，同步 L2 8 键回源 | 1541.354 → 1370.005 ns | -11.12% | 7337 → 6945 |
| 32 线程，同步 L2 单键热命中 | 6.400 → 6.977 ns | +9.01% | 0 → 0 |
| 32 线程，同步 L2 8 键热命中 | 187.905 → 201.724 ns | +7.35% | 416 → 416 |
| 32 线程，同步 L2 单键回源 | 3073.662 → 3066.557 ns | -0.23% | 1776 → 1552 |
| 32 线程，同步 L2 8 键回源 | 6754.463 → 5804.041 ns | -14.07% | 7317 → 6944 |
| 32 线程，真实 Redis 单键回源 | 367.056 → 316.335 µs | -13.82% | 15767 → 15571 |
| 32 线程，真实 Redis 8 键回源 | 430.109 → 412.720 µs | -4.04% | 62279 → 61887 |

单键分配降低约 12.6%，同步批量分配降低约 5.1%。32 线程批量回源的两个候选样本均快于两个基线样本，单键同步回源没有明确耗时收益。热命中首轮均值变慢，基线批量自身为 169.355/206.455 ns；热路径方法、Cache 和 Snapshot 经 javap 归一化对照字节码相同，保留这些负向数据。

针对首轮热命中负向结果，用相同类与夹具进行独立 B/C/C/B 专项复核，改为 6×1s 预热、6×1s 测量，其他设置不变。单键均值 6.433 → 6.620 ns（+2.90%），8 键均值 198.734 → 201.308 ns（+1.30%）；分配仍为 0/416 B/op。批量基线为 176.588/220.881 ns，候选为 194.261/208.355 ns，对照方向反转，未稳定复现超过 5% 的回退。专项结果没有替换首轮数据，也不与不同预热设置的首轮样本合并成通过结论。原始文件为 `/private/tmp/config-converge-hot-check-{1-B,2-C,3-C,4-B}.json`。

原始结果为 `/private/tmp/config-converge-final-cpu1-{1-B,2-C}.json`、`/private/tmp/config-converge-final-{cpu32,redis32}-{1-B,2-C,3-C,4-B}.json`。共享加载重构前 `d41e901` 的整体 5% 目标没有因本轮局部收益而通过；本轮主要降低分配与批量加载协调成本，全局在途登记仍是剩余成本之一。端到端 P99 与 24 小时长稳尚未验证。交付继续使用 [#48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

### cad170f 的验证与性能边界（收敛前）

编译及 82 项定向回归通过，零失败/错误/跳过：原有 68 项，加 `ConfigStorageFollowupReviewTest` 的 14 项。原四个反例全部通过，新增覆盖独立 fallback Context/取消/错误、成功缓存与结束其他等待者、重试、过期负缓存遇到 L2 错误、失效/关闭、写入与共享值计算竞态、部分 Key 提前完成、跨组失败释放。日志为 `/private/tmp/config-storage-followup-final.log`。

定向命令：`mvn -o -Dtest=ConfigStorageFollowupReviewTest,LocalCacheClusterConfigStorageTest,ConfigStorageMonitorTest,LocalCacheClusterConfigStorageLifecycleTest,EventBusStorageManagerTest,RedisClusterCacheTest test`。

三版本对照：D 为共享加载重构前 `d41e901`，A 为跟进修复前 `7982443`，B 为当前实现的非 fallback 路径。使用同一编译夹具、JDK 21、32 线程、1 fork、4×1s 预热、5×1s 测量、512 MiB 固定堆和 GC profiler；CPU 顺序 D/A/B，真实 Redis 5 顺序 D/A/B/B/A/D，每个 worker 使用不同 Storage id 和独立 L1。期间没有本任务的并行构建或压测。

| 场景 | 耗时 D → A → B | B 相对 A | B 相对 D | 分配 B/op D → A → B |
| --- | ---: | ---: | ---: | ---: |
| 同步 L2，单键热命中 | 7.861 → 6.621 → 7.273 ns | +9.85% | -7.48% | 0 → 0 → 0 |
| 同步 L2，8 键热命中 | 189.599 → 195.629 → 223.296 ns | +14.14% | +17.77% | 416 → 416 → 416 |
| 同步 L2，单键持续回源 | 254.801 → 3119.776 → 3150.394 ns | +0.98% | +1136.42% | 384 → 2201 → 1776 |
| 同步 L2，8 键持续回源 | 1317.601 → 7491.932 → 6893.614 ns | -7.99% | +423.19% | 2704 → 7985 → 7337 |
| 真实 Redis，单键持续回源 | 391.623 → 397.466 → 405.691 µs | +2.07% | +3.59% | 14332 → 16199 → 15755 |
| 真实 Redis，8 键持续回源 | 465.522 → 504.431 → 495.883 µs | -1.69% | +6.52% | 57559 → 63015 → 62195 |

Redis 数据为各版本两个样本的均值，保留所有结果；B 的单键样本为 417.805/393.577 µs，批量为 501.102/490.664 µs。单键/批量分配相对 A 分别减少约 19.3%/8.1%，但耗时没有全面改善。**最多 5% 的目标仍未通过**：热命中批量及持续 miss 均存在超过边界的结果，不能用某个 Redis 场景低于 5% 代替整体通过，也不能将协调成本全部归于本机波动。

原始结果为 `/private/tmp/config-followup-cpu-{D,A,B}.json`、`/private/tmp/config-followup-redis-{1-D,2-A,3-B,4-B,5-A,6-D}.json`。B 测量完成后仅修复 `fallbackRead` 的过期负缓存重试循环并清理 import；最终编译的 `javap -c -p` 对照确认外部类仅该方法字节码变化，性能夹具不传 fallback，因此复用未变化路径的测量，不把未测的 fallback 成本算作已验证。字节码记录为 `/private/tmp/config-followup-{measured,final}-bytecode.txt`。设备消息端到端 P99 与 24 小时 churn/GC/RSS 尚未验证。交付使用 [#48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

### 7982443 的验证与性能边界（本轮修改前）

编译及 68 项定向测试通过：观测/故障边界 28、存储 15、管理器 4、共享加载生命周期 19、Redis 原子读取删除 2。覆盖版本和值原子发布、迟到结果、重叠批量与单键合并、Context、重试、取消、写入/失效/关闭、底层超时后恢复及不同 hash 序列化器。回归使用 Sinks、虚拟时间和确定性门闩，验证实际订阅边界；合法的下游转换异常传播给调用方，不计作 L2 加载错误。

定向命令：`mvn -o -Dtest=LocalCacheClusterConfigStorageTest,ConfigStorageMonitorTest,EventBusStorageManagerTest,LocalCacheClusterConfigStorageLifecycleTest,RedisClusterCacheTest test`。

性能目标仍为最多 5% 影响，**当前尚未满足**。比较本轮修改前 `d41e901` 与当前实现：JDK 21、JMH 32 线程、1 fork、4×1s 预热、5×1s 测量、512 MiB 固定堆及 GC profiler；两边使用同一份编译后的夹具。每个回源 worker 使用不同 Storage id 和独立 L1，防止同 id 的实例规范化改变夹具含义。

| 同步 L2 场景 | ns/op 基线 → 当前 | 耗时变化 | B/op 基线 → 当前 |
| --- | ---: | ---: | ---: |
| 单键热命中 | 7.538 → 6.158 | -18.31% | 0 → 0 |
| 8 键热命中 | 223.688 → 192.609 | -13.89% | 416 → 416 |
| 单键持续回源 | 228.721 → 3111.673 | +1260.47% | 384 → 2201 |
| 8 键持续回源 | 1253.991 → 7025.527 | +460.25% | 2704 → 8087 |

真实 Redis 5 使用同一夹具，以 A/B/B/A 顺序各运行两次；32 个 worker 各持有独立 L1，测量持续 miss，复用相同不变的 L2 数据。两次均值如下：

| 真实 Redis 场景 | µs/op 基线 → 当前 | 耗时变化 | B/op 基线 → 当前 |
| --- | ---: | ---: | ---: |
| 单键持续回源 | 323.425 → 343.306 | +6.15% | 14315 → 16227 |
| 8 键持续回源 | 374.788 → 401.893 | +7.23% | 57575 → 63031 |

热命中没有观测工作或新增分配；持续 miss 的协调成本和分配明显增加，不能只用热路径收益宣称 5% 通过。Storage 没有新增加载超时定时器；加入者复用已有加载对象，单一共享请求省去多请求合并步骤，成功释放不创建取消异常。增加的 Pending、订阅/结果协调、引用管理与 manager 在途登记用于共享加载和生命周期保证，不能归咎于观测或全部归咎于本机波动。真实 Redis 两个候选样本单键为 359.951/326.662 µs，存在波动，保留全部结果而不择优。

夹具为 `ConfigStorageConcurrencyBenchmark` 和 `ConfigStorageRedisBenchmark`；原始结果 `/private/tmp/config-lifecycle-final-cpu-{baseline,candidate}.json`、`/private/tmp/config-lifecycle-final-redis-{1-baseline,2-candidate,3-candidate,4-baseline}.json`。历史观测精简结果不能当作本轮重构的证据。真实设备消息端到端 P99 与 24 小时 churn/GC/RSS 仍未验证。交付继续使用 [#48](https://github.com/jetlinks/jetlinks-supports/pull/48)，性能门槛保持未通过。

## 一级缓存观测与一致性

观测只回答三个运维问题：缓存是否持续膨胀、配置读取是否因错误使用旧结果、本地失效及监听链路是否异常。实现归属 `src/main/java/org/jetlinks/supports/config/`，复用：

`org.jetlinks:type=ConfigStorage,name=LocalCacheClusterConfigStorage`

正常命中、成功/空加载、取消和失效通知均不执行观测工作。已删除读取/加载抽样、成功/取消/在途/耗时统计、随机数判定和 Loader 的计时/终态观测字段、CAS 及回调。5% 性能影响仍为验收上限；实现不通过增加采样间隔、特判 L2 或额外包装来维持画像统计。

### 运维契约

`CacheSize` 仍表示 storage 数量。`Statistics` 为标准 JMX `CompositeData`：

| 运维目标 | 字段 | 解释与动作 |
| --- | --- | --- |
| 容量趋势 | `storageCount`、`localEntryCount`、`countCapturedAt` | 条目包括正/负缓存、加载中和未清理项；结合堆内存趋势检查生命周期、TTL 和清理策略，不据此伪造精确内存字节数。 |
| 加载与旧结果兜底 | `loadErrors`、`staleFallbacks` | 异常路径全量记录；旧值兜底包括返回旧正值或旧负结果。正常取消不是错误。 |
| 本地失效 | `invalidationErrors`、`notificationSubscriptionActive` | 接收通知时的本地失效处理异常与本地订阅句柄状态；存活不证明跨节点通知必达，尚未发起订阅也不等于故障。 |
| 外部监听器 | `listenerErrors` | 与本地失效异常分开；监听器抛错不代表已完成的本地失效失败。 |
| 最近错误 | `lastErrorStage`、`lastErrorType`、`lastErrorAt` | 原子发布有界的阶段/异常类型/时间快照，不保留 Throwable、Key 或配置值；详细原因沿原异常传播和现有日志定位。 |

`loadErrors` 包含 L2/fallback 及本地加载结果处理的异常，不是 Redis 请求失败数；读取者自己的下游转换异常继续传播，不计入该数。共享请求的一次错误只记录一次，重试的新请求独立记账，不采样、不换算为请求总数或失败率。订阅创建失败和规模扫描失败分别标记 `subscription`、`countScan` 阶段；加载、本地失效、监听器分别为 `load`、`invalidation`、`listener`。

`invalidationErrors` 覆盖 `handleNotify` 中接收通知后的本地处理错误；它与监听器失败一样，原本由异步处理器捕获，不能只依赖调用方发现。公开写入、主动失效与通知发布失败继续沿原调用链传播，不为凑齐操作成功/失败次数增加包装或改写控制流。

上下文还包含 `schemaVersion`、`instanceId`、`capturedAt`、`namespace`、`idleTtlMillis`。计数器为生命周期累计值，判断新故障应比较增量/最近时间，重启通过 instanceId 区分；计数与容量为弱一致统计。最近错误不随一次成功自动清除，也不单独代表当前健康状态。

首次完整规模扫描前 `localEntryCount=-1`、`countCapturedAt=0`。后台扫描只汇总已有 map 大小，复用共享调度器，每秒最多处理 65536 个 storage 或协作预算 5 ms，目标每 30 秒刷新，单轮 120 秒超时。部分、超时或失败扫描不覆盖上一次完整结果及其时间；getter 不启动扫描。不再暴露扫描进度字段，观察方应根据快照时间判断新鲜度。

`inspect(storageId,maxKeys)` 仅用于按需排障，最多返回 128 条本地 Key 状态，Key 最多 512 字符并标记截断；不返回值、不创建 storage、不续期、不访问 L2，不周期遍历 Key。版本冲突、无对应 storage 的广播、正常过期与刷新不计为默认异常。旧 `search` 保持原语义，不作为被动采集来源。

中间形态的读取开关、采样字段、加载画像和过程计数均未随本 PR 发布，直接移除，不增加兼容分支。MBean 固定 schema 只缓存字段名与类型；关闭时释放扫描、订阅及自己注册的 MBean，不抢占同名注册。前端专用视图不在本次范围，通用 MBean 工具仍可读取属性。

### 一致性与业务路径

失效移除加载中的条目，旧请求允许结束；迟到的单键/批量结果只更新脱离当前 map 的旧对象。L2 写成功后才更新 L1，未订阅/失败/取消不暴露未提交值；L2 已提交但通知失败仍向调用方报错。业务所需的 `doOnSuccess` 保留，不添加观测操作符或包装。

默认单键读取省去空 fallback 的 `switchIfEmpty`。显式 fallback 在 L2 空结果后按各自调用执行，普通读者只共享 L2；缓存成功结果和较新写入可结束仍在等待该 Key 的调用。批量按开始时的状态认领 Pending，较新写入已准备的等待者终态优先；显式负值标记不出现在返回的 Values 中。重试和重复订阅状态独立。TTL 为空闲过期，不保证热 Key 的最大陈旧时间，也不提供硬容量上限。

观测不增加 Key 级字段、加载对象、响应式包装或逐条 TraceHolder 埋点；共享加载的状态仅服务业务正确性和取消传播。现有异常传播和链路追踪保留，正常业务路径不为观测创建 span。

### 观测精简阶段验证与性能预算（共享加载重构前）

以下数据对应此前的观测精简实现，保留用于解释历史性能判断，不代表当前共享加载实现；当前证据见上文。

Maven 编译及 47 项定向测试通过（观测/故障边界 28、存储 15、含真实 Redis 的管理器 4），零失败/错误/跳过。覆盖写入与失效竞态、迟到正负结果、共享加载、重试/取消、fallback、下游交付错误、被动查看、快照超时及 MBean 生命周期；新增分别验证本地失效、监听器、订阅创建及扫描失败的计数/阶段与恢复语义。

```bash
mvn -o -Dtest=LocalCacheClusterConfigStorageTest,EventBusStorageManagerTest,ConfigStorageMonitorTest test
```

生产代码 SHA-256 前 16 位：`ConfigStorageMonitor=df7af0408f401af7`、`EventBusStorageManager=7a5e411fd4e26750`、`LocalCacheClusterConfigStorage=a6b0d24bed9c2653`。源码核对确认读取 API、L1 命中、TTL 与负缓存读取路径和 808de76 逐字一致，复用已有相应边界证据；本轮重新验收修改过的加载路径，不重跑无关构建。

加载性能对照使用同一候选编译的 `ConfigStorageConcurrencyBenchmark` 夹具和依赖，只切换 `a160079` 无观测基线与候选生产类，JDK 21、512 MiB 固定堆。共享一个 manager，每线程清空自己的 L1，L2 为同步内存代理；不包含通知、扫描/JMX 轮询或真实 Redis/设备链路。每进程 1 fork、4 × 1 s 预热、5 × 1 s 测量，GC profiler；1/8 线程各 3 组、32 线程 4 组，按预定顺序交替运行。

| 线程 | 场景 | 平均吞吐，百万次/s：基线 → 当前 | 吞吐变化 | 各组变化 |
| --- | --- | ---: | ---: | --- |
| 1 | 单键加载 | 10.259 → 11.608 | +13.14% | +15.10%、+11.14%、+13.32% |
| 8 | 单键加载 | 62.970 → 72.965 | +15.87% | +2.09%、+20.17%、+25.76% |
| 32 | 单键加载 | 108.648 → 118.343 | +8.92% | +2.73%、-3.52%、+12.06%、+25.29% |
| 1 | 8 键批量加载 | 2.407 → 2.426 | +0.81% | +0.36%、-0.42%、+2.57% |
| 8 | 8 键批量加载 | 10.188 → 12.970 | +27.30% | +17.24%、+33.27%、+32.07% |
| 32 | 8 键批量加载 | 22.243 → 20.263 | **-8.90%** | -3.43%、-9.76%、-4.62%、-16.97% |

单键分配约 448 → 384 B/op，批量约 2696 → 2704 B/op。另以 32 线程、2 forks、3 × 1 s 预热、4 × 1 s 测量运行 sample-time：单键 P99 为 0.792 → 0.833 µs（+5.18%），批量 P99 为 2.664 → 2.624 µs（-1.50%）。结果保留在 `/private/tmp/config-operational-{singleReload,batchReload8}-t{1,8,32}-{baseline,optimized}-{pair}.json`、`/private/tmp/config-operational-sample-{baseline,optimized}.json`。

**5% 上限尚未通过全部场景验收**：32 线程批量吞吐回退 8.90%，单键采样 P99 增加 5.18%。不能用其他场景的均值改善、置信区间重叠或本机波动解释为通过。正常加载源码已没有观测工作；对照仍包含本 PR 的批量重复订阅/失效隔离等一致性修复，现有数据不能把差异单独归因于观测。保留这些正确性修复，不为基准回退到会污染缓存的实现。最终测试源码补充旧正值/负值兜底后，单独重跑 `ConfigStorageMonitorTest` 28 项通过，生产指纹未变化，复用前述存储/管理器证据。

上一版 808de76 的 32 线程顺序对照曾有 -6.32% 热命中、-13.38% 单键加载，以及 +5.18% 单键加载 P99；同一代码重复运行也有约 27% 波动。这些是已撤除加载抽样的旧版本数据，不作为当前版本结论；原始数据仍在 `/private/tmp/config-five-put-*.json` 和 `/private/tmp/config-five-final32-*.json`，未将失败改写为通过。

### 32 线程下降归因复核（共享加载重构前）

复核对象仍是 `a160079` 与 `d2982b6` 的冻结生产类和同一夹具，未修改实现、未停止本地服务。主机为 18 物理/逻辑核、64 GiB，32 个工作线程已超过核心数。接电且起止 `pmset` 无温控/性能告警。每进程 1 fork、6 × 1 s 预热、6 × 1 s 测量，512 MiB 堆；每秒采集进程累计 CPU 差与 `vm_stat`，CPU 的 100% 表示一个核心。进程采样包含启动/预热/测量，不能精确当作 JMH 每次测量的 CPU。

预定顺序为同基线 A/A、四组 AB/BA 交替、同基线 A/A；单键 sample-time 再执行 B/A/A/B。保留全部结果：

| 批量对照 | 基线 → 当前，百万次/s | 吞吐变化 |
| --- | ---: | ---: |
| A → B，第 1 组 | 23.433 → 21.273 | -9.22% |
| B → A，第 2 组 | 22.174 → 25.438 | +14.72% |
| A → B，第 3 组 | 21.396 → 22.795 | +6.54% |
| B → A，第 4 组 | 19.880 → 21.305 | +7.17% |
| 四组均值 | 21.721 → 22.703 | +4.52% |

同代码基线 A/A 为 24.275 → 22.693（-6.52%）及 22.514 → 22.100（-1.84%）；连续两次当前版本为 21.273 → 25.438（+19.58%）。因此，原先 -8.90% 没有稳定复现，本机及 JVM 进程间波动足以改变对照方向；新均值改善也不等于证明严格 5% 上限。

环境证据：上述唯一负向 A/B 中，较慢 B 运行的后台 CPU 峰值为 378.8%（约 3.8 核），其中短暂 Java 进程约 292.5%；后续同代码 B 运行的后台峰值仅约 107.1%，吞吐高了 19.58%。整个序列还观察到 IDE、桌面、Node/Java、Docker 等后台活动。未停止/暂停这些进程，也未追溯捕获已退出 Java 的具体服务身份。与此同时，后台较安静的基线也有较低吞吐，故不能将差异全部归于本地服务。旧压测没有进程采集，无法证明当时 -8.90% 由哪个服务直接造成。全程无 swapout/pageout，只有合计 36 个 16 KiB 页的 swapin，不支持持续换页压力；起止温控查询也不能排除未公开的瞬时频率/调度变化。

代码与 GC：批量正常路径没有观测调用、共享计数写入或额外锁；每线程访问自己的 L1。相对基线，保留只读目标、每订阅结果隔离、去重/负值处理，`get + putIfAbsent` 与 `remaining` 取代原先对共享目标的 `remove`，属于一致性修复的实现差异。分配约多 8 B/op（约 0.30%），不能仅凭此归因 8.90% 吞吐。复测批量分配约 54–69 GB/s，每个 6 秒测量段有 1048–1317 次 GC、GC time 约 530–672 ms；该短链路处于高分配/频繁 GC 条件，对调度及 JVM 进程差异敏感。GC time 没有持续随当前版本增加，不支持“当前版 GC 明显恶化”的结论；JIT/调度/频率各自贡献尚未隔离，不能将其写成已证实根因。

单键 sample-time 的基线两次 P99 都为 0.792 µs，当前为 0.834、0.792 µs；按原始样本合并后分别为 0.792、0.833 µs（仍 +5.18%），没有用最后一次相同结果替换合并结果。独立 `System.nanoTime()` 探针最小正步长为 41/42 ns，原直方图也集中在 0.750、0.791/0.792、0.833/0.834、0.875 µs 档位；原先差值只有一个时钟档位，无法据此精确区分 5% 边界，但也不能自动视为通过。

原始文件位于 `/private/tmp/config-load-diagnose/`：`ab-{1..8}-{A,B}.json`、`aa-{start,end}-{1,2}-A.json`、`p99-{1..4}-{A,B}.json`、对应 `*-system.jsonl`、`metadata.json`、`thermal-after.txt`；固定顺序脚本 `/private/tmp/config-load-diagnose.py`，时钟探针 `/private/tmp/NanoTimeResolutionProbe.java`。结论为环境/进程波动已获证据，稳定批量回退尚未确认；不为波动结果修改正确性实现，不再追加相同本机对照直至变绿。**严格 5% 验收继续保持未通过**，尤其不能把一个时钟档位的 P99 差异当作精确的生产开销承诺。

容量扫描的访问/时间预算未扩大，删除了仅用于显示进度的字段和更新。真实 Redis/设备链路 CPU/input、端到端 P99 A/B、24 小时 churn/GC/RSS 尚未验证；本机同步 L2 基准不能保证所有生产负载的性能上限。交付继续使用 [jetlinks-supports #48](https://github.com/jetlinks/jetlinks-supports/pull/48)。

## 双、三键全 L1 命中复核

本节及后续通用 L1/设备链路对照是历史优化证据，测试制品早于当前共享加载实现。

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
