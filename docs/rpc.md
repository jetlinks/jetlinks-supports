# 原生 RSocket RPC 与节点发现

新实现独立于 ScaleCube Services，平台默认引擎仍由原有配置选择。Core 与 Supports 须配套部署；新旧引擎的协议和注解不互通。原生传输使用 RSocket 1.1.5，SDK 与平台须保持版本一致。

| 组件 | 职责 |
| --- | --- |
| `NodeDiscovery` | 本节点公告、不可变成员视图与变化；不保存服务目录或业务状态 |
| `RpcTransport` | 连接、双向帧传输、四种 RSocket 交互及传输资源 |
| `RpcServiceRegistry` | 沿用 RPC 的注册、查询、选择、监听方法；提供原子初始服务视图 |
| `DefaultRpcManager` | 组合发现与传输，管理 peer、目录就绪、调用与 Context 租约 |
| `RpcBodyCodec` / `ContextCodec` | 分别编解码业务对象与调用上下文 |

`ScaleCubeNodeDiscovery` 仅使用当前 ScaleCube Cluster。它可拥有独立成员实例，也可借用已启动的 `ExtendedCluster`；借用时须独占该成员的应用公告元数据，关闭时仅撤销公告。后续发现 provider 无须依赖 RPC 实现。

## 接入

公共接口显式标记 `@RpcApi`，返回 `Mono<T>` 或 `Flux<T>`，至多一个参数。`@RpcMethod` 可指定稳定的方法名；重载必须指定不同名字。默认方法名为 Java 方法名，API 名默认为接口完整类名。

```java
@RpcApi("metrics/latest")
public interface LatestMetrics {
    Mono<String> latest(String deviceId);
}

NodeAnnouncement local = new NodeAnnouncement(
    new NodeIdentity(nodeId, UUID.randomUUID()),
    "production", "iot", Map.of("role", "iot"), Map.of());

DefaultRpcManager manager = new DefaultRpcManager(
    local,
    new ScaleCubeNodeDiscovery(clusterConfig),
    new RSocketRpcTransport(2),
    new JavaRpcBodyCodec(), // 平台使用 SerializeRpcBodyCodec
    ContextCodec.DEFAULT, // 需要鉴权、locale、trace 时注入平台 DefaultContextCodec
    RpcOptions.defaults(URI.create("tcp://127.0.0.1:0")));

Disposable registration = manager.registerService(implementation);
Mono<Void> startup = manager.start();
Mono<String> result = manager.getService("iot-2", LatestMetrics.class)
    .flatMap(service -> service.latest("device-1"));
// 宿主组合并订阅 startup；停机组合 manager.stop()，撤销单个服务调用 registration.dispose()。
```

生产监听地址与公告地址按部署配置：`endpoints["rpc"]` 可指定对端可达地址，省略时使用实际绑定地址。`0.0.0.0` 监听需显式配置公告地址。节点运行期 UUID 每次重启重新生成；逻辑 nodeId 在同一 namespace 内唯一。

查询可以返回空；先等待就绪可使用 `watchServices(Api.class)`。它原子交付初始视图，之后交付最新完整视图；慢观察者允许跳过中间版本。`unresolved` 非空表示已知成员的目录尚未确认，空集合也仅表示本机已知成员已解析。`listen` 提供这些视图间的服务增删差异，不保证逐个中间事件。

发现流失败会终止远端服务发现并撤销连接。现有及后续观察者、远端查询和旧句柄调用均收到原始错误，不发布正常的空目录；本地调用仍遵循本地注册生命周期。恢复发现须由宿主重建 manager。

`getServices` / `selectService` 枚举远端；定点 `getService(currentServerId(), ...)` 支持本地调用。本地调用保留当前 Reactor Context，不进行序列化。无类型 `getServices()` 返回元数据，调用需使用带 Class 的查询。自定义 Collector 在每次业务订阅时重新选择，默认无 key 轮询、有 key 使用 rendezvous hash。

注册、撤销和目录传播最终收敛。只有协议和目录均就绪的 peer 才可路由；重复逻辑 nodeId 从路由中排除。断线建立新 peer 代际，旧回调不能污染新连接。没有自动业务重试，超时或断线不能证明对端未执行，业务幂等由调用方负责。

## ByteBuf 与 Context 所有权

| 边界 | 所有权 |
| --- | --- |
| 单请求 ByteBuf 参数 | 调用方保留原引用；每次订阅运行时 retain 一个可读区租约；服务只借用，返回结果必须另 retain |
| channel 的 ByteBuf 元素 | 逐元素转移所有权，接收方消费后 release；未交付元素由对应队列 owner 回收 |
| ByteBuf 结果 | 转移给调用者，调用者负责 release |
| `RpcFrame` | 拥有 data/metadata 各一份引用；只 release frame；`retainedDuplicate` 保持共同生命周期和独立索引 |
| Context capture | 每次订阅创建，成功、失败、超时或取消统一 dispose；不得跨请求缓存 |

单请求的调用方须在订阅取得租约前保持原缓冲区可访问，完成或取消后可以释放自己的引用。借用参数的内容视为只读；异步保留到本次调用结束之后必须额外 retain。零拷贝描述载荷透传，不表示完全无对象分配。

响应帧在 `onNext` 时转移所有权，此前的取消清理由传输方负责。runtime 解码并释放已经收到的帧。单次请求响应由局部操作符统一仲裁解码交付、超时与取消：解码期间保持帧有效，超时或取消先赢时回收晚到的帧及未交付的解码结果；成功交付后结果归调用者。channel 和业务异步队列回收自己尚未交付的元素，按各自边界配置 discard，避免对同一引用重复清理。调用者额外添加队列、超时等操作符时，仍须管理该边界持有的 ByteBuf。

```java
// 服务端：参数是借用视图；结果转移所有权。
public Mono<ByteBuf> echo(ByteBuf request) {
    return Mono.just(request.retainedSlice());
}
```

`SerializedContext.writeTo(ByteBuf)` 追加到 writerIndex，不释放目标；`ContextCodec.deserializeFrom` 同步读取借用区域，不保存或释放它。旧文本实现默认 UTF-8 桥接，二进制实现同时覆写 `formatId`；连接双方必须使用相同格式。

平台 `DefaultContextCodec` 二进制路径直接使用现有 serializer，不经 Base64 或完整中间数组，透传鉴权、locale、递归校验和 trace。安全 Context 在业务参数反序列化及同步服务方法入口前恢复；损坏的外层 Context 或安全恢复失败会拒绝调用。旧文本入口继续用于旧引擎。

## 执行与预算

单个 peer 默认一条双向 TCP 连接，目录与业务流复用该连接；单线程控制面不调度业务数据。路由与服务方法绑定在本地索引读取，不查询远程注册中心。方法签名及路由前缀在注册/句柄创建时解析；业务方法由 MethodHandle 调用。

绑定期仅共享不可变的方法计划。Context、ByteBuf 租约、并发槽位、超时及当前 peer 均在每次订阅时取得；复用同一调用 Publisher 也不会复用这些请求资源。融合操作符保留这一边界，不能把计划绑定到某次鉴权快照或某一代连接，否则会破坏权限更新和故障迁移。

客户端句柄为每个方法预绑定交互类型、路由前缀和 trace 名称；调用时只查询一个方法索引。unary 使用单个局部 `RpcCallMono` 操作符获取订阅 Context、同步路由并完成序列化、响应解码、超时和租约清理。捕获 Context 或构造 source 期间发生取消时，先完成初始化再归还借用资源；结果解码期间的资源由解码 owner 持有。业务 Publisher 和流/channel 的独立队列、背压边界继续由其原 owner 管理。

服务端在注册时建立字节路由索引，与本地服务表作为同一不可变快照发布、撤销。索引仅持有注册期编码的只读堆字节，寿命随快照回收；请求查找借用载荷区域，不缓存或释放请求缓冲区。协议保持 Modified-UTF 路由格式，接收时仅扫描长度并匹配字节，不逐请求创建路由字符串。

没有 Context 字节时直接借用完整可读路由区域，并复用 Netty 空缓冲区，省去两个区域视图；查找不移动输入索引。非空 Context 仍借用当前请求区域，保留独立索引与原所有权边界。

入站 unary 使用 `RpcServerMono` 合并槽位、Context 恢复、结果编码、空响应和错误回包。同步方法入口仍处于 tracing 订阅边界中；安全恢复先于参数解码与方法调用。每次订阅持有请求帧引用，初始化结束前取消不会提前归还借用内存；至多缓存一个响应，空结果和错误回包也等待下游 demand，交付或取消后统一归还请求与槽位。

默认并发上限为每节点 1024 个入站及出站调用租约，单帧预算 16MiB，连接/目录时限 5 秒，重连间隔 1 秒。通过 `RpcOptions` 调整。业务请求默认不设置框架超时；`requestTimeout == null` 时不创建请求计时任务，也可以显式配置正的 Duration 作为该 manager 的请求响应时限。长流及 channel 的总时限仍由业务决定；阻塞业务须在明确的业务适配边界切换 scheduler。

业务调用方可按操作的含义设置超时：

```java
service.latest("device-1").timeout(Duration.ofSeconds(3));
```

业务超时通过取消传播到远端 Publisher，并归还调用租约、Context 与请求引用；取消不代表对端已经完成的副作用被回滚。不设置任何请求超时时，未结束的调用持续占用槽位和资源，直到完成、取消、断连或 manager 停止。

`RpcTransport.bind/connect` 接收本节点的 `maxPayloadBytes`，限制单个载荷的 data 与 metadata 总字节数。RSocket 双端使用共同的 16KiB 物理帧上限；分片重组上限为业务预算加协议开销，最低为一个物理帧，解码时再精确校验业务预算。小预算节点因此可以正常接收对端的合法物理帧，同时拒绝超出自身业务预算的载荷。

响应式操作符保留 Context、取消与资源边界，空 Context codec 走直接路径。传输双端 ZERO_COPY；SETUP 保留一次连接级快照，避免连接建立阶段的取消和复用竞态。业务调用不复制 ByteBuf 载荷。

一期不含 RSocket resume、业务重试、自动连接池扩容、存储复制及选主。可选 token 为内部 peer 的共享凭据，默认空值仅适合受信网络；当前 TCP adapter 不提供 TLS，不能直接暴露到公网。Java/FST 对象编解码仅接受受信节点，不作为公共不可信数据格式。
