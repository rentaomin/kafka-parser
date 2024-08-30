package cn.rtm.kafkaParser.protocol.parser;


import cn.rtm.kafkaParser.protocol.exception.ProtocolParseException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.requests.RequestHeader;
import java.nio.ByteBuffer;


/**
 *
 * Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
 *   request_api_key => INT16
 *   request_api_version => INT16
 *   correlation_id => INT32
 *   client_id => NULLABLE_STRING
 * 请求头解析器
 * @author rtm
 * @date 2023/08/08
 */
public class RequestHeaderParser implements KafkaProtocolParser<RequestHeaderData> {


    @Override
    public RequestHeaderData parsePacket(ByteBuffer buffer, short version) throws ProtocolParseException {
        RequestHeader header = RequestHeader.parse(buffer);
        return header.data();
    }

}
