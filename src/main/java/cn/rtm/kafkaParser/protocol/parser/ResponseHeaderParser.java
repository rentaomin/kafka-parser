package cn.rtm.kafkaParser.protocol.parser;

import cn.rtm.kafkaParser.protocol.exception.ProtocolParseException;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import java.nio.ByteBuffer;

/**
 *  响应头解析器
 *
 * Response Header v0 => correlation_id
 *   correlation_id => INT32
 *
 *  Response Header v1 => correlation_id TAG_BUFFER
 *   correlation_id => INT32
 * @author rtm
 * @date 2024/08/15
 */
public class ResponseHeaderParser implements KafkaProtocolParser<ResponseHeaderData> {


    @Override
    public ResponseHeaderData parsePacket(ByteBuffer buffer, short headerVersion) throws ProtocolParseException {
        return new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }


}
