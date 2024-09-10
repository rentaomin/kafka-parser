package cn.rtm.kafkaParser.protocol.parser.req;

import cn.rtm.kafkaParser.protocol.ProtocolParser;

/**
 *  请求数据包解析器
 * @param <P> 需要解析的数据包内容
 * @param <M> 解析后的数据包内容
 */
public interface RequestParser<P,M> extends ProtocolParser<P,M> {

}
