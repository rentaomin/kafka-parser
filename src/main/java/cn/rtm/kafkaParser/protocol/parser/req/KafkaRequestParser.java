package cn.rtm.kafkaParser.protocol.parser.req;

import cn.rtm.kafkaParser.protocol.Message;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.ProtocolContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

/**
 *  Kafka 请求数据包解析器,主要提供请求解析分发，采用 kafka-client 自身能力实现数据解析，避免重复
 *  造轮子，且可保证与官方 api 同步更新支持，可无需关心版本兼容问题
 *
 *  <ul>
 *  <li> 方法 {@linkplain #parseRequest(ByteBuffer)} 实现 Kafka 请求数据包的解析,
 *  <li> 请求数据包内容格式为： RequestMessage =》 Size RequestHeader RequestPayload
 *  <li> 方法 {@linkplain #parseRequestHeader(ByteBuffer)} 实现 RequestHeader 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain RequestHeader#parse(ByteBuffer)} 执行真正的解析
 *  <li> 方法 {@linkplain #parseRequestPayload(ByteBuffer, RequestHeader)} 实现 RequestPayload 解析，具体实现
 *  委托 Kafka-client 源码 {@linkplain AbstractRequest#parseRequest(ApiKeys, short, ByteBuffer)} 执行真正的解析
 *  <li> 方法 {@linkplain #buildParsedRequestMessage(RequestHeader, ApiMessage)} 实现请求数据包解析结果的组装
 *  </ul>
 */
public class KafkaRequestParser implements RequestParser<ProtocolMessage, Message> {

    private Logger log = LoggerFactory.getLogger(getClass());

    private ProtocolMessage data;

    private ProtocolContext protocolContext;

    public KafkaRequestParser(ProtocolContext protocolContext) {
        this.protocolContext = protocolContext;
    }

    @Override
    public Message parse(ProtocolMessage packet) {
        this.data = packet;

        ByteBuffer buffer = packet.rawDataWithNoLength();
        if (packet.isRequestPacket()) {
           return this.parseRequest(buffer);
        }
        return null;
    }


    /**
     *  解析请求数据包内容
     * @param buffer 请求数据包
     * @return 返回解析后的请求数据包内容
     */
    private Message parseRequest(ByteBuffer buffer) {

        RequestHeader header = parseRequestHeader(buffer);

        ApiMessage payload = parseRequestPayload(buffer, header);

        Message message = buildParsedRequestMessage(header,payload);

        return message;
    }


    /**
     *  解析请求头数据包内容
     * @param buffer 请求数据包中请求头数据包
     * @return 返回解析后的请求头数据包内容
     */
    private RequestHeader parseRequestHeader(ByteBuffer buffer) {

        RequestHeader header = null;
        try {
            header = RequestHeader.parse(buffer);
        } catch (Exception e) {
            log.error("解析:{} 请求头出错！",data.requestDesc(),e);
        }
        return header;
    }


    /**
     *  解析请求数据包 payload 内容
     * @param buffer 请求数据包 payload 内容
     * @param header 解析完成的请求头信息
     * @return 返回解析完成的请求 payload 内容
     */
    private ApiMessage parseRequestPayload(ByteBuffer buffer, RequestHeader header) {
        if (header == null || isUnsupportedApiVersionsRequest(header)) {
            return null;
        }
        ApiKeys apiKey = header.apiKey();
        short apiVersion = header.apiVersion();
        ApiMessage apiMessage = null;
        try {
            apiMessage = AbstractRequest.parseRequest(apiKey, apiVersion, buffer).request.data();
        } catch (Exception e) {
            log.error("Error getting request for apiKey: " + apiKey +
                    ", apiVersion: " + header.apiVersion() +
                    ", connectionId: " + header.clientId() +
                    ", listenerName: " + header.apiKey(), e);
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


    /**
     *  构建解析完成的请求内容
     * @param header 解析的请求头内容
     * @param payload 解析的请求 payload 内容
     * @return 返回解析完成的请求数据包内容
     */
    private Message buildParsedRequestMessage(RequestHeader header, ApiMessage payload) {
        if (header == null) {
            return null;
        }
        Message message = new Message();
        message.setRequestHeader(header);
        message.setRequestMessage(payload);
        message.setRequestLength(data.getLength());
        message.setOriginData(data);
        message.setRequestApi(buildRequestApi(header,payload));
        message.setRequestData(Boolean.TRUE);
        message.setParsedRequest(Boolean.TRUE);
        protocolContext.addParam(data.getResponseAckId(), message);
        return message;
    }

    private String buildRequestApi(RequestHeader header,ApiMessage payload) {
        StringBuilder apiInfo = new StringBuilder(32);
        ApiKeys apiKeys = header.apiKey();
        String slash = " ";
        apiInfo.append("Kafka");
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
        apiInfo.append(data.getRequestUrl());
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
