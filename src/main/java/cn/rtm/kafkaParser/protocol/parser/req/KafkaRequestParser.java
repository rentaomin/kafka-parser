package cn.rtm.kafkaParser.protocol.parser.req;

import cn.rtm.kafkaParser.protocol.ProtocolContext;
import cn.rtm.kafkaParser.protocol.enums.ProtocolType;
import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.parser.AbstractProtocolParser;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import java.nio.ByteBuffer;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

/**
 *  Kafka 请求数据包解析器,主要提供请求解析分发，采用 kafka-client 自身能力实现数据解析，避免重复
 *  造轮子，且可保证与官方 api 同步更新支持，可无需关心版本兼容问题
 *
 *  <ul>
 *  <li> 请求数据包内容格式为： RequestMessage =》 Size RequestHeader RequestPayload
 *  <li> 方法 {@linkplain #parseHeader(ByteBuffer)} 实现 RequestHeader 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain RequestHeader#parse(ByteBuffer)} 执行真正的解析
 *  <li> 方法 {@linkplain #parseBody(RequestHeader, ByteBuffer)} 实现 RequestPayload 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain AbstractRequest#parseRequest(ApiKeys, short, ByteBuffer)} 执行真正的解析
 *  <li> 方法 {@linkplain #buildParsedMessage(RequestHeader, ApiMessage)} 实现请求数据包解析结果的组装
 *  </ul>
 */
public class KafkaRequestParser extends AbstractProtocolParser<RequestHeader, ApiMessage, KafkaProtocolParsedMessage> {


    public KafkaRequestParser(ProtocolContext protocolContext) {
        super(protocolContext);
    }

    @Override
    protected RequestHeader parseHeader(ByteBuffer buffer) {
        if (!isRequestPacket()) {
            return null;
        }
        RequestHeader header = null;
        try {
            header = RequestHeader.parse(buffer);
        } catch (Exception e) {
            log.error("解析:{} 请求头出错！",getCommonData().requestDesc(),e);
        }
        return header;
    }


    @Override
    protected ApiMessage parseBody(RequestHeader parsedHeader, ByteBuffer buffer) {
        if (parsedHeader == null || isUnsupportedApiVersionsRequest(parsedHeader)) {
            return null;
        }
        ApiKeys apiKey = parsedHeader.apiKey();
        short apiVersion = parsedHeader.apiVersion();
        ApiMessage apiMessage = null;
        try {
            apiMessage = AbstractRequest.parseRequest(apiKey, apiVersion, buffer).request.data();
        } catch (Exception e) {
            log.error("Error getting request for apiKey: " + apiKey +
                    ", apiVersion: " + parsedHeader.apiVersion() +
                    ", connectionId: " + parsedHeader.clientId() +
                    ", listenerName: " + parsedHeader.apiKey(), e);
        }
        return apiMessage;
    }


    /**
     *  判断是否为不支持的 API 版本请求
     * @param header 请求头信息
     * @return 返回 true 则不支持， 反之 false
     */
    private boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey().equals(API_VERSIONS) && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }


    @Override
    protected KafkaProtocolParsedMessage buildParsedMessage(RequestHeader header, ApiMessage body) {
        if (header == null) {
            return null;
        }
        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = new KafkaProtocolParsedMessage();
        kafkaProtocolParsedMessage.setRequestHeader(header);
        kafkaProtocolParsedMessage.setRequestMessage(body);
        kafkaProtocolParsedMessage.setRequestLength(getCommonData().getLength());
        kafkaProtocolParsedMessage.setOriginData(getCommonData());
        kafkaProtocolParsedMessage.setRequestApi(buildRequestApi(header));
        kafkaProtocolParsedMessage.setRequestData(Boolean.TRUE);
        kafkaProtocolParsedMessage.setParsedRequest(Boolean.TRUE);
        kafkaProtocolParsedMessage.setStartTime(this.startParseTime);
        protocolContext.addParam(getCommonData().getResponseAckId(), kafkaProtocolParsedMessage);
        return kafkaProtocolParsedMessage;
    }


    /**
     *  构建请求 api 语义标识
     * @param header 请求头信息
     * @return 返回当前请求标识语义
     */
    private String buildRequestApi(RequestHeader header) {
        StringBuilder apiInfo = new StringBuilder(32);
        ApiKeys apiKeys = header.apiKey();
        String slash = " ";
        apiInfo.append(ProtocolType.KAFKA.getName());
        apiInfo.append(slash);
        apiInfo.append(apiKeys.name);
        apiInfo.append("(");
        apiInfo.append(apiKeys.id);
        apiInfo.append(")");
        apiInfo.append(slash);
        apiInfo.append("v");
        apiInfo.append(header.data().requestApiVersion());
        apiInfo.append(slash);
        apiInfo.append("Api Request =>{ ");
        apiInfo.append(getCommonData().getRequestUrl());
        apiInfo.append(slash);
        apiInfo.append("Header-Version=");
        apiInfo.append("v");
        apiInfo.append(header.headerVersion());
        apiInfo.append(slash);
        apiInfo.append("clientId=");
        apiInfo.append(header.clientId());
        apiInfo.append(slash);
        return apiInfo.toString();
    }
}
