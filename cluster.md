# JetLinks 集群说明

## 集群管理

集群管理使用`scalecube`框架,基于`gossip`协议实现,核心类`ExtendedCluster`,
通过此类可进行`集群节点事件监听`,`节点间点对点,gossip通信`等操作.

## RPC通信

集群间RPC通信使用`scalecube`框架,基于`rsocket`进行通信,核心类`RpcManager`,
可动态注册,获取`rpc服务`进行服务调用.

应用场景:
在一些`有状态`的业务功能,在接收到前端请求后需要同时操作集群下所有(部分)节点的功能时,可以使用
`RpcManager`来进行操作.
如: 网络组件,规则引擎动态启动停止；
集群下事件总线(EventBus)的消息传递；
集群下设备会话管理协调；等等。

注意: rpc基于`scalecube`框架,定义服务时会有一些限制:

1. 服务接口必须注解`io.scalecube.services.annotations.Service`
2. 服务接口方法必须注解`io.scalecube.services.annotations.ServiceMethod`
3. 方法的返回值必须时`Mono`或`Flux`类型
4. 方法的参数只能有一个.