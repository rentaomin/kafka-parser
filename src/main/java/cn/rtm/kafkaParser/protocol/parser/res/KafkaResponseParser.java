package cn.rtm.kafkaParser.protocol.parser.res;

import cn.rtm.kafkaParser.protocol.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.exception.ProtocolParseException;
import cn.rtm.kafkaParser.protocol.ProtocolContext;
import cn.rtm.kafkaParser.protocol.parser.ResponseHeaderParser;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;


/**
 *  Kafka 响应数据包解析器,主要提供响应数据包解析分发，采用 kafka-client 自身能力实现数据解析，避免重复
 *  造轮子，且可保证与官方 api 同步更新支持，可无需关心版本兼容问题
 *
 *  <ul>
 *  <li> 方法 {@linkplain #parseResponse(ByteBuffer)} 实现 Kafka 响应数据包的解析
 *
 *  <li> 响应数据包内容格式为： ResponseMessage =》 Size ResponseHeader ResponsePayload
 *
 *  <li> 方法 {@linkplain #parseResponseHeader(ByteBuffer, RequestHeader)} 实现 ResponseHeader 解析,此处
 *  自定义{@linkplain ResponseHeaderParser#parsePacket(ByteBuffer, short)} 实现数据包装代理 ,具体实现
 *  委托 Kafka-client 源码 {@linkplain ResponseHeaderData#read(Readable, short)}执行真正的解析； 解析响应数据包
 *  需要依赖对应的请求数据包解析内容，主要通过{@linkplain #getRequestHeaderByResponseAckId()} 获取请求解析结果，主要通过
 *  ack-number 和 correlationId 进行匹配请求-响应，请求数据包的 ack-number 也是响应数据包的 seq-number
 *
 *  <li> 方法 {@linkplain #parseResponsePayload(ByteBuffer, RequestHeader)} 实现 ResponsePayload 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain AbstractResponse#parseResponse(ApiKeys, ByteBuffer, short)}  执行真正的解析
 *
 *  <li> 方法 {@linkplain #buildParsedResponseMessage(ResponseHeaderData, ApiMessage)} 实现响应数据包解析结果的组装
 *  </ul>
 */
public class KafkaResponseParser implements ResponseParser<ProtocolMessage, KafkaProtocolParsedMessage> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private ProtocolMessage data;

    private final ProtocolContext protocolContext;

    public KafkaResponseParser(ProtocolContext protocolContext) {
        this.protocolContext = protocolContext;
    }


    @Override
    public KafkaProtocolParsedMessage parse(ProtocolMessage packet) {
        this.data = packet;

        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = null;
        ByteBuffer buffer = packet.rawDataWithNoLength();
        if (packet.isResponsePacket()) {
            kafkaProtocolParsedMessage = this.parseResponse(buffer);
        }
        return kafkaProtocolParsedMessage;
    }


    /**
     *  解析响应数据包
     * @param buffer 响应数据包
     * @return 返回解析后的内容
     */
    private KafkaProtocolParsedMessage parseResponse(ByteBuffer buffer){

        RequestHeader requestHeader = this.getRequestHeaderByResponseAckId();

        ResponseHeaderData responseHeader = this.parseResponseHeader(buffer, requestHeader);

        ApiMessage responseMessage = this.parseResponsePayload(buffer, requestHeader);

        return this.buildParsedResponseMessage(responseHeader, responseMessage);
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
            log.error("未找到解析的请求头信息，跳过解析，当前请求：{}", data.requestDesc());
            return null;
        }
        return requestHeader;
    }


    /**
     *  解析响应数据包请求头信息
     * @param buffer 响应数据包
     * @param requestHeader 响应数据包对应的请求数据包请求头信息
     * @return 返回解析后的响应数据包请求头信息
     */
    private ResponseHeaderData parseResponseHeader(ByteBuffer buffer, RequestHeader requestHeader) {
        if (buffer == null || requestHeader == null) {
            return null;
        }

        ResponseHeaderData responseHeader = null;
        try {
            responseHeader = new ResponseHeaderParser()
                    .parsePacket(buffer, requestHeader.apiKey()
                            .responseHeaderVersion(requestHeader.apiVersion()));
        } catch (ProtocolParseException e) {
            log.error("解析响应数据包请求头出错：{} ", data.requestDesc());
        }

        if (responseHeader != null && requestHeader.correlationId() != responseHeader.correlationId()) {
            log.error("解析的请求 correlationId:{} 和响应 correlationId: {} 内容不匹配！",
                    requestHeader.correlationId(), responseHeader.correlationId());
            return null;
        }
        return responseHeader;
    }


    /**
     *  解析响应数据包 payload 内容
     * @param buffer 响应数据包
     * @param requestHeader  响应数据包对应的请求数据包请求头信息
     * @return 返回解析后的响应数据包 payload 内容
     */
    private ApiMessage parseResponsePayload(ByteBuffer buffer, RequestHeader requestHeader) {
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
            log.error("解析响应数据包 payload 出错:{} ", data.requestDesc(), e);
        }
        return responseMessage;
    }


    /**
     *  构建解析完成的响应消息内容
     * @param responseHeader 解析的响应数据包 header 内容
     * @param responseMessage 解析的响应数据包 payload 内容
     * @return 返回解析的完整数据包内容
     */
    private KafkaProtocolParsedMessage buildParsedResponseMessage(ResponseHeaderData responseHeader, ApiMessage responseMessage) {
        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = getParsedRequestMessage();
        if (kafkaProtocolParsedMessage == null) {
            return null;
        }
        kafkaProtocolParsedMessage.setResponseHeader(responseHeader);
        kafkaProtocolParsedMessage.setResponseMessage(responseMessage);
        kafkaProtocolParsedMessage.setResponseLength(data.getLength());
        kafkaProtocolParsedMessage.setParsedResponse(Boolean.TRUE);
        kafkaProtocolParsedMessage.setParseComplete(Boolean.TRUE);
        kafkaProtocolParsedMessage.setRequestData(Boolean.FALSE);
        return kafkaProtocolParsedMessage;
    }


    /**
     * 获取该响应数据包对应的请求数据包解析内容
     * @return 返回包含当前响应数据包对应的请求数据包解析内容
     */
    public KafkaProtocolParsedMessage getParsedRequestMessage() {
        return this.protocolContext.getParamAs(data.getAcknowledgementNumber(), KafkaProtocolParsedMessage.class);
    }
}
