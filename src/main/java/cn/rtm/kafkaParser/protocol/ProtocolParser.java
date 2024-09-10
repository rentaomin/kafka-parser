package cn.rtm.kafkaParser.protocol;


/**
 *  该接口为顶层协议解析接口，提供基础的数据包解析接口
 * @param <P> 需要解析的数据包
 * @param <M> 解析后的数据内容
 */
@FunctionalInterface
public interface ProtocolParser<P,M> {

    /**
     *  解析数据
     * @param packet
     * @return
     */
    M parse(P packet);

}
