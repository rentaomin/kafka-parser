package cn.rtm.kafkaParser.protocol.parser.res;

import cn.rtm.kafkaParser.protocol.ProtocolParser;

/**
 *  响应数据包解析器
 * @param <P> 需要解析的数据包内容
 * @param <M> 解析后的数据包内容
 */
public interface ResponseParser<P,M> extends ProtocolParser<P,M> {


}
