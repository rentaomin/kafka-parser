package cn.rtm.protocol.parser.protocols.kafka.parser.response;

import cn.rtm.protocol.parser.ProtocolParseException;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParser;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Readable;
import java.nio.ByteBuffer;

/**
 *  响应头解析器,负责解析响应数据包，真实解析委托 {@link ResponseHeaderData#read(Readable, short)} 执行真正的解析
 *
 * Response Header v0 => correlation_id
 *   correlation_id => INT32
 *
 *  Response Header v1 => correlation_id TAG_BUFFER
 *   correlation_id => INT32
 * @author rtm
 * @date 2024/08/15
 */
public class KafkaResponseHeaderParser implements KafkaProtocolParser<ResponseHeaderData> {


    @Override
    public ResponseHeaderData parsePacket(ByteBuffer buffer, short headerVersion) throws ProtocolParseException {
        return new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion);
    }

}
