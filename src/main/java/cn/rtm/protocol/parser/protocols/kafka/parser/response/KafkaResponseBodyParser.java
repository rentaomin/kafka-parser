package cn.rtm.protocol.parser.protocols.kafka.parser.response;

import cn.rtm.protocol.parser.AbstractProtocolParser;
import cn.rtm.protocol.parser.ProtocolContext;
import cn.rtm.protocol.parser.ProtocolParseException;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParsedMessage;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import java.nio.ByteBuffer;


/**
 *  Kafka 响应数据包解析器,主要提供响应数据包解析分发，采用 kafka-client 自身能力实现数据解析，避免重复
 *  造轮子，且可保证与官方 api 同步更新支持，可无需关心版本兼容问题
 *
 *  <ul>
 *
 *  <li> 响应数据包内容格式为： ResponseMessage =》 Size ResponseHeader ResponsePayload
 *
 *  <li> 方法 {@linkplain #parseHeader(ByteBuffer)} 实现 ResponseHeader 解析,此处
 *  自定义{@linkplain KafkaResponseHeaderParser#parsePacket(ByteBuffer, short)} 实现数据包装代理 ,具体实现
 *  委托 Kafka-client 源码 {@linkplain ResponseHeaderData#read(Readable, short)}执行真正的解析； 解析响应数据包
 *  需要依赖对应的请求数据包解析内容，主要通过{@linkplain #getRequestHeaderByResponseAckId()} 获取请求解析结果，主要通过
 *  ack-number 和 correlationId 进行匹配请求-响应，请求数据包的 ack-number 也是响应数据包的 seq-number
 *
 *  <li> 方法 {@linkplain #parseBody(ResponseHeaderData, ByteBuffer)} 实现 ResponsePayload 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain AbstractResponse#parseResponse(ApiKeys, ByteBuffer, short)}  执行真正的解析
 *
 *  <li> 方法 {@linkplain #buildParsedMessage(ResponseHeaderData, ApiMessage)} 实现响应数据包解析结果的组装
 *  </ul>
 */
public class KafkaResponseBodyParser extends AbstractProtocolParser<ResponseHeaderData, ApiMessage, KafkaProtocolParsedMessage> {


    public KafkaResponseBodyParser(ProtocolContext protocolContext) {
        super(protocolContext);
    }


    @Override
    protected ResponseHeaderData parseHeader(ByteBuffer buffer) {
        if (!isResponsePacket()) {
            return null;
        }
        RequestHeader requestHeader = this.getRequestHeaderByResponseAckId();
        if (buffer == null || requestHeader == null) {
            return null;
        }

        ResponseHeaderData responseHeader = null;
        try {
            responseHeader = new KafkaResponseHeaderParser()
                    .parsePacket(buffer, requestHeader.apiKey()
                    .responseHeaderVersion(requestHeader.apiVersion()));
        } catch (ProtocolParseException e) {
            log.error("解析响应数据包请求头出错：{} ", getCommonData().requestDesc());
        }

        if (responseHeader != null && requestHeader.correlationId() != responseHeader.correlationId()) {
            log.error("解析的请求 correlationId:{} 和响应 correlationId: {} 内容不匹配！",
                    requestHeader.correlationId(), responseHeader.correlationId());
            return null;
        }
        return responseHeader;
    }


    /**
     *  根据响应数据包 ack-id 查找对应的请求数据包请求头信息
     * @return 返回请求数据包请求头信息
     */
    private RequestHeader getRequestHeaderByResponseAckId() {
        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = getParsedRequestMessage();
        if (kafkaProtocolParsedMessage == null) {
            return null;
        }
        RequestHeader requestHeader = kafkaProtocolParsedMessage.getRequestHeader();
        if (requestHeader == null) {
            log.error("未找到解析的请求头信息，跳过解析，当前请求：{}", getCommonData().requestDesc());
            return null;
        }
        return requestHeader;
    }


    /**
     * 获取该响应数据包对应的请求数据包解析内容
     * @return 返回包含当前响应数据包对应的请求数据包解析内容
     */
    public KafkaProtocolParsedMessage getParsedRequestMessage() {
        return protocolContext.getParamAs(getCommonData().getAcknowledgementNumber(), KafkaProtocolParsedMessage.class);
    }


    @Override
    protected ApiMessage parseBody(ResponseHeaderData header, ByteBuffer buffer) {
        RequestHeader requestHeader = this.getRequestHeaderByResponseAckId();
        if (requestHeader == null) {
            return null;
        }
        ApiMessage responseMessage = null;
        try {
            AbstractResponse response = AbstractResponse.parseResponse(requestHeader.apiKey(), buffer, requestHeader.apiVersion());
            if (response == null) {
                return null;
            }
            responseMessage = response.data();
        } catch (Exception e) {
            log.error("解析响应数据包 payload 出错:{} ", getCommonData().requestDesc(), e);
        }
        return responseMessage;
    }


    @Override
    protected KafkaProtocolParsedMessage buildParsedMessage(ResponseHeaderData header, ApiMessage body) {
        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = getParsedRequestMessage();
        if (kafkaProtocolParsedMessage == null) {
            return null;
        }
        kafkaProtocolParsedMessage.setResponseHeader(header);
        kafkaProtocolParsedMessage.setResponseMessage(body);
        kafkaProtocolParsedMessage.setResponseLength(getCommonData().getLength());
        kafkaProtocolParsedMessage.setParsedResponse(Boolean.TRUE);
        kafkaProtocolParsedMessage.setParseComplete(Boolean.TRUE);
        kafkaProtocolParsedMessage.setRequestData(Boolean.FALSE);
        kafkaProtocolParsedMessage.setEndTime(this.startParseTime);
        return kafkaProtocolParsedMessage;
    }
}
