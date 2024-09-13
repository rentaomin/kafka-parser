package cn.rtm.protocol.parser;

/**
 *  协议解析协调器，负责协调各解析组件解析协议内容
 *
 * @param <P> 原生协议数据包
 * @param <M> 解析后的消息
 */
@FunctionalInterface
public interface ProtocolParseHandler<P,M> {

    /**
     *  对外提供解析协议入口，外部应用通过该接口进行解析协议内容
     * @param packet 捕获的数据协议包内容
     * @return 返回解析后的内容
     */
    M handle(P packet);

}
