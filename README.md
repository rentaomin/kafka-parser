# 实现 Kafka 协议解析

## 设计原理
- 主要组件为 ProtocolParseHandler 负责提供解析入口，组装解析依赖组件
- 组件 ProtocolParser 负责解析工作
- 组件 PacketReassemble 负责重组 协议数据包，避免 tcp 分片传输包不全
- 组件 DataParseExtractor 负责对解析后的数据，根据业务需要提取数据

## 目前实现了 kafka 协议数据包解析，也可以基于现有接口解析其它协议
## 使用方法
```text
通过 ProtocolParseHandler#handle(packet) 调用
```
# 可以运行于 SpringBoot 程序中，通过自动配置注入 
```text
 1、 cn.rtm.kafkaParser.protocol.config.KafkaProtocolConfig 实现 解析依赖 bean 自动注入
 2、使用基础 ProtocolHandler new 进行实例化，示例位于：KafkaParser
```
