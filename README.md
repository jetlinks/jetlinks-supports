# jetlinks-supports
[![Maven Central](https://img.shields.io/maven-central/v/org.jetlinks/jetlinks-supports.svg)](http://search.maven.org/#search%7Cga%7C1%7Cjetlinks-supports)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/https/oss.sonatype.org/content/repositories/snapshots/org/jetlinks/jetlinks-supports/maven-metadata.xml.svg)](https://oss.sonatype.org/content/repositories/snapshots/org/jetlinks/jetlinks-supports)
[![Build Status](https://travis-ci.com/jetlinks/jetlinks-supports.svg?branch=master)](https://travis-ci.com/jetlinks/jetlinks-supports)
[![codecov](https://codecov.io/gh/jetlinks/jetlinks-supports/branch/master/graph/badge.svg)](https://codecov.io/gh/jetlinks/jetlinks-supports)

JetLinks 提供的一些默认实现以及集成

## 设备会话读取

`getDeviceSessionInfo(deviceId)` 保留现有尽力读取行为。需要区分远端失败和设备无会话时，
使用 `getDeviceSessionSnapshot(deviceId)` 并检查 `DeviceSessionReadResult.isComplete()`。
结果包含成功会话、本次请求节点和失败节点；失败节点不贡献部分会话，成功空会话仍计为读取成功。
节点集合在每次订阅时从已发现的会话服务确定，包含本地节点；它不是全局事务或物理集群拓扑证明。
只需要统计值时使用 `getDeviceSessionStatistics(deviceId)`：本地和远端节点各读取指定设备一次，
节点侧直接汇总连接指标，不构造或传输连接身份、地址和明细；请求端再合并同一设备在不同节点上的统计。
新增内部 `deviceStatistics` RPC 返回 Core 的 `DeviceSessionStatistics`，通过自定义 `Externalizable`
版本与位标记编码原始计数、时间和非零分布桶，平均值与P95由原始事实重建。读取时计算，不持久化。
实现复用原 RPC、追踪、超时及取消机制，不新增常驻缓存或后台任务。
节点读取信任内部 RPC 生成的设备与节点标识，仅在正常终止后接受至多一条会话；错误前的部分数据仍丢弃。
公共覆盖结果只在查询边界创建一次，单节点直接返回本地结果。失败节点始终在结果中报告，逐次堆栈日志置于 debug。
统计接口成功无会话返回零计数；空响应、旧节点缺少统计方法、解码失败及调用错误均表示该节点读取失败，
不能降级为成功的零值。外部旧管理器可由 Core 默认实现适配明细，覆盖仍沿用原结果，不会把未知升级为完整。
使用方必须检查 `DeviceSessionStatistics.isComplete()`；完整性仅限本次已发现的服务，部署时须同步 Core 与 Supports。

## 物模型定义

声明适配器可以继承 `JavaBeanCommandSupport`，通过受保护的 `ResolvableType` 模板构造器
及既有 `applyMetadata` 钩子取得最终命令元数据，避免另写命令 ID、参数和注解解析。
实例执行仍使用 `copyWith(target)`；模板不保存请求身份或业务数据。

物模型主要由: 属性(properties),功能(function),事件(event).组成

默认 Codec 将根 `null`、空串、空白及 JSON `null` 解析为空模型；对已有模型调用
`fromJson(null)` 会清除旧身份、扩展及全部目录。目录缺失或为 `null` 表示没有定义，
但目录中的每个条目必须是对象，空项、标量和数组条目会返回带 i18n code 的格式错误。
复制和合并结果持有独立的关系声明及递归模块对象，各层扩展 Map 独立，嵌套扩展值沿用原值语义。
core 的 `SimpleDeviceMetadata` 同样支持属性、功能、事件、标签、关系与模块的完整 JSON 往返，
使用 core 已注册的数据类型，不依赖 supports 的实现类。

默认 Codec 同时保留模型身份、`expands` 和可选的 `relations`、`modules` 目录。
关系目录复用 core 的 `RelationMetadata`，JSON 为带 `targetType` 的列表；只描述另一端类型及关系含义，不加载目标实例。原关系 SPI 的 Map 接口不变。
缺省目录为空，旧模型的 JSON 不额外输出空目录；设备和通用物模型使用同一解析实现。
`DefaultThingsMetadata.addRelations` 接受类型化的 `RelationMetadata` 集合，追加时保留已解析或延迟解析的原目录，并复制集合；调用方无需拼装 JSON 或加载关联实例。
与包含这些通用目录的 core 版本配套使用，语义词及未知扩展键按原值往返。

`JavaBeanCommandSupport` 复用 core 的 `@FunctionSemantics`、`@SemanticRole`、`@SemanticTypes`、`@GraphSubject` 和 `@Presentation` 生成标准扩展。处理方法声明与原 `@CommandHandler(expands=...)` 合并时拒绝已知语义冲突；未分类的命令不推断为 query。原 codec 保留主体标记、记录 identity、未知角色以及主值对应的采样时间路径，不建立独立语义解析器。
命令参数及显式引用命令类的适配通过 `MetadataUtils` 使用 core 的统一默认合并策略；`ExpandsMergeProvider` 声明默认规则，应用可通过 `ExpandsMerges.register` 注册并释放自定义规则。注册在构造命令元数据前生效，已构造的模型保持原值。补齐已声明的嵌套对象并拒绝实际语义冲突，普通方法选项优先级、原值顺序及方法输出保持原行为；supports 不维护自己的注册表、语义键或集合白名单。

物模型使用场景:
1. 前端通过模型定义动态展示设备运行状态或者设备操作界面
2. 服务端可通过统一的API获取设备模型并进行相关操作,如: 在发送设备消息时进行参数校验,
在收到设备消息进行类型转换处理.

数据结构:

    {
        "id":"设备ID",
        "name":"设备名称",
        "properties":[...属性],
        "functions":[...功能],
        "events":[...事件]
    }

### 属性
用于定义设备属性,运行状态等如: 设备SN,当前CPU使用率等.
平台可主动下发消息获取设备属性,设备也通过事件上报属性.

数据结构: 

    {
       "id": "cpu_usage", //属性标识
       "name": "CPU使用率",
       "valueType": {   //值类型
         "type": "double", //类型标识,见类型表
         "maxValue":100,
         "minValue":0,
         "unit":"percent",     //单位
         "expands":{"key1":"value1"} //其他自定义拓展定义
       },
       "expands":{"key1":"value1"} //其他自定义拓展定义
     }

### 功能
用于定义设备功能,平台可主动调用,例如: 播放语音,开关操作等.

数据结构:

    {
       "id": "playVoice", //功能标识
       "name": "播放声音", //名称
       "inputs": [  //输入参数
         {
           "id": "text",
           "name": "文字内容",
           "valueType": { //参数类型
             "type": "string"
           },
           "expands":{"key1":"value1"} //其他自定义拓展定义
         }
       ],
       "output": { //输出
         "type": "boolean" //输出类型
       },
       "expands":{"key1":"value1"} //其他自定义拓展定义
     }

### 事件
用于定义设备事件, 如: 定时上报设备属性, 设备报警等.

数据结构:

    {
       "id": "fire_alarm", //事件标识
       "name": "火警",
       "valueType": {
         "type": "object",  //对象(结构体)类型
         "properties": [    //对象属性(结构与属性定义相同)
           {
             "id": "location",
             "name": "地点",
             "valueType": {
               "type": "string"
             }
           },
           {
             "id": "lng",
             "name": "经度",
             "valueType": {
               "type": "double"
             },
             "expands":{"gis":"lng"} //其他自定义拓展定义
           },
           {
             "id": "lat",
             "name": "纬度",
             "valueType": {
               "type": "double"
             },
              "expands":{"gis":"lat"} //其他自定义拓展定义
           }
         ]
       },
       "expands":{"key1":"value1"} //其他自定义拓展定义
    }

### 数据类型

所有类型共有属性:
- id 唯一标识
- name 名称
- description 描述
- expands 自定义配置

类型定义

1. int 整型
2. long 长整型
3. float 单精度浮点型
4. double 双精度浮点型

    以上均为数字类型,共有属性:
    - max 最大值
    - min 最小值
    - unit 单位
     
     例:
     
        {
        "type":"double",
        "max":100,
        "min":0,
        "unit":"percent",
        "expands":{"readonly":true}
        }
    
5. boolean 布尔类型
    
     属性
      - trueText 为true时的文本,默认为`是`
      - falseText 为false时的文本,默认为`否`
      - trueValue  为true时的值,默认为`true`
      - falseValue 为false时的值,默认为`false`
      
      例:
      
        {
            "type":"boolean",
            "trueText":"开启",
            "falseText":"关闭",
            "trueValue":"1",
            "falseValue":"0"
        }
      
6. string 字符类型

      例:
      
        {
           "type":"string",
           "expands":{"maxLen","255"}
        }
        
7. enum   枚举类型

    属性:
     - elements (Element)枚举中的元素
        
    Element:
     - value 枚举值
     - text 枚举文本
     - description 说明
         
    例:
    
        {
            "type":"enum",
            "elements":[
                {"value":"1","text":"正常"},
                {"value":"-1","text":"警告"},
                {"value":"0","text":"未知"}
            ]
        }
        
8. date   时间类型
    
    属性:
      - format 格式,如: yyyy-MM-dd
      - tz 时区,如: Asia/Shanghai
    
    例:
    
        {
            "type":"date",
            "format":"yyyy-MM-dd",
            "tz": "Asia/Shanghai"
        }

9. password 密码类型
    
    与string类型相同
    
10. file    文件类型
    
    属性:
      - bodyType 类型: url,base64,binary
      
    例:
    
        {
           "type":"file",
           "bodyType":"url"
        }
      
11. array 数组(集合)类型

    属性:
      - eleType 元素类型
      
    例:
        
        {
            "type":"array",
            "eleType":{ 
                "type":"string"
            }
        }
        
12. object  对象(结构体)类型

     属性:
        - properties 属性列表
        
     例:
        
        {
            "type":"object",
            "properties":[
                {
                 "id": "location",
                 "name": "地点",
                 "valueType": {
                   "type": "string"
                 }
               },
               {
                 "id": "lng",
                 "name": "经度",
                 "valueType": {
                   "type": "double"
                 },
                 "expands":{"gis":"lng"}
               },
               {
                 "id": "lat",
                 "name": "纬度",
                 "valueType": {
                   "type": "double"
                 },
                  "expands":{"gis":"lat"}
               }
            ]
        }

