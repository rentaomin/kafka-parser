package cn.rtm.protocol.parser.protocols.kafka;


import cn.rtm.protocol.parser.ProtocolParseException;
import java.nio.ByteBuffer;

/**
 * Kafka 协议解析器
 * @author rtm
 * @param <K> 解析后的协议包内容
 */
@FunctionalInterface
public interface KafkaProtocolParser<K> {

    /**
     * 解析指定版本号的协议包内容，解析器只负责解析对应的协议包内容,子类需要判断解析器是否支持指定版本号 API 解析
     * <p>
     * 如：请求头只负责解析请求头，响应头只负责解析响应头，解析器不关心解析内容，解析器只负责解析协议包内容
     * </p>
     * @param payload 协议包内容
     * @param version 协议包版本号
     * @return 返回解析后的内容
     * @throws ProtocolParseException 解析请求协议包异常，抛出该异常
     */
    K parsePacket(ByteBuffer payload, short version) throws ProtocolParseException;

}
