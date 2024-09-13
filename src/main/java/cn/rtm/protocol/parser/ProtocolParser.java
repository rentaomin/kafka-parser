package cn.rtm.protocol.parser;


/**
 *  该接口为顶层协议解析接口，提供基础的数据包解析接口
 * @param <P> 需要解析的数据包
 * @param <M> 解析后的数据内容
 */
@FunctionalInterface
public interface ProtocolParser<P,M> {

    /**
     *  解析协议数据包
     * @param packet 需要解析的数据包
     * @return 返回解析后的内容
     */
    M parse(P packet);

}
