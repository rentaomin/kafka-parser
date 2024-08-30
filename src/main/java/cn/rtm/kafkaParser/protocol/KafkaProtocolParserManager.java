package cn.rtm.kafkaParser.protocol;

import cn.rtm.kafkaParser.protocol.exception.ProtocolParseException;
import cn.rtm.kafkaParser.protocol.parser.ResponseHeaderParser;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;


/**
 *
 *
 *  1、重组 tcp 分片请求
 *
 *  2、识别请求和响应关系：
 *      a、请求的 seq + payLoadLength = 响应的 ack, 解析请求，存储key: seq+payLoadLength, value: 解析的请求内容，其中 payLoadLength=rawDataLength + 4;
 *      b、解析响应，先根据 ack 获取对应的请求内容，如果不存在，则跳过解析
 *
 * @description:  对外提供解析入口
 * @author rtm
 * @date 2024/8/21
 */
public class KafkaProtocolParserManager {

    private Logger log = LoggerFactory.getLogger(getClass());

    private ProtocolMessage data;

    /**
     *  存储解析完成的请求数据包信息
     */
    private final static Map<Long,Message> parsedMessage = new ConcurrentHashMap<>(16);


    public Message parse(ProtocolMessage data){
        if (data == null || !data.isKafkaPacket()) {
            log.info("当前数据包不是 Kafka 协议的包！");
            return null;
        }
        if (!data.isCompletePacket()) {
            log.info("当前数据包不是完整的包！");
            return null;
        }
        this.data = data;
        return this.startParse();
    }


    /**
     *  开始解析数据包内容
     * @return 返回解析的结果
     */
    public Message startParse() {
        ByteBuffer buffer = data.rawDataWithNoLength();

        if (data.isRequestPacket()) {
            this.parseRequest(buffer);
        } else if (data.isResponsePacket()) {
            this.parseResponse(buffer);
        } else {
           return null;
        }
        Message message = parsedMessage.get(data.getAcknowledgementNumber());

        if (message != null && message.isParseComplete() && message.getResponseLength() > 5000) {
            log(data.getSrcIp(),data.getSrcPort(),data.getDestIp(),data.getDestPort(),data.getLength(),
                    data.getSequenceNumber(),data.getAcknowledgementNumber(),message);
        }
        return message;
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

        this.cacheRequestMessage(data.getResponseAckId(), message);

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
        return message;
    }


    /**
     *  缓存解析的请求数据包信息
     * @param ackId 请求包 ack-id
     * @param message 解析后的请求数据包内容
     */
    private void cacheRequestMessage(long ackId,Message message) {
        parsedMessage.put(ackId, message);
    }

    /**
     *  判断是否为不支持的 API 版本请求
     * @param header 请求头信息
     * @return 返回 true 则不支持， 反之 false
     */
    private boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey().equals(API_VERSIONS) && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }


    private Message parseResponse(ByteBuffer buffer){

        RequestHeader requestHeader = this.getRequestHeaderByResponseAckId();

        ResponseHeaderData responseHeader = this.parseResponseHeader(buffer, requestHeader);

        ApiMessage responseMessage = this.parseResponsePayload(buffer, requestHeader, responseHeader);


//        if (data.getLength() > 5000) {
//            MemoryRecords records = (MemoryRecords) ((FetchResponseData) responseMessage).responses().get(0).partitions().get(0).records();
//            AbstractIterator<MutableRecordBatch> iterator = records.batchIterator();
//            while (iterator.hasNext()) {
//                MutableRecordBatch next = iterator.next();
//                Iterator<Record> recordIterator = next.iterator();
//                while (recordIterator.hasNext()) {
//                    Record record = recordIterator.next();
//                    ByteBuffer key = record.key();
//                    ByteBuffer value = record.value();
//                    try {
//                        System.out.println(new String(value.array(),"UTF-8"));
//                    } catch (UnsupportedEncodingException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//        }

        Message message = this.buildParsedResponseMessage(responseHeader, responseMessage);

        this.cacheRequestMessage(data.getAcknowledgementNumber(), message);

        return message;
    }


    /**
     *  根据响应数据包 ack-id 查找对应的请求数据包请求头信息
     * @return 返回请求数据包请求头信息
     */
    private RequestHeader getRequestHeaderByResponseAckId() {
        Message message = parsedMessage.get(data.getAcknowledgementNumber());
        if (message == null) {
            return null;
        }

        RequestHeader requestHeader = message.getRequestHeader();
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
        if (requestHeader == null) {
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

        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            log.error("解析的请求 correlationId:{} 和响应 correlationId: {}内容不匹配！",
                    requestHeader.correlationId(), responseHeader.correlationId());
            return null;
        }
        return responseHeader;
    }


    /**
     *  解析响应数据包 payload 内容
     * @param buffer 响应数据包
     * @param requestHeader  响应数据包对应的请求数据包请求头信息
     * @param responseHeader 解析后的响应数据包请求头信息
     * @return 返回解析后的响应数据包 payload 内容
     */
    private ApiMessage parseResponsePayload(ByteBuffer buffer, RequestHeader requestHeader, ResponseHeaderData responseHeader) {
        ApiMessage responseMessage = null;
        try {
            responseMessage = AbstractResponse.parseResponse(requestHeader.apiKey(), buffer, requestHeader.apiVersion())
                    .data();
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
    private Message buildParsedResponseMessage(ResponseHeaderData responseHeader, ApiMessage responseMessage) {
        Message message = parsedMessage.get(data.getAcknowledgementNumber());
        if (message == null) {
            return null;
        }
        message.setResponseHeader(responseHeader);
        message.setResponseMessage(responseMessage);
        message.setResponseLength(data.getLength());
        message.setParseComplete(Boolean.TRUE);
        return message;
    }


    /**
     *  日志记录解析内容
     * @param srcIp 请求源 ip 地址
     * @param srcPort 请求源 ip 端口
     * @param destIp 目标 ip 地址
     * @param destPort 目标 ip 端口
     * @param realLength 请求携带的数据包大小
     * @param sequenceNumber 数据包请求序列号
     * @param acknowledgmentNumber 响应数据包确认序列号
     * @param message 解析完成的结果
     */
    public void log(String srcIp, int srcPort,String destIp, int destPort,
                    int realLength, long sequenceNumber, long acknowledgmentNumber, Message message) {
        StringBuilder logMsg = new StringBuilder();
        logMsg.append("-------- start ---------\n");
        logMsg.append("srcIp: "+srcIp);
        logMsg.append("\n");
        logMsg.append("srcPort: "+srcPort);
        logMsg.append("\n");
        logMsg.append("destIp: "+destIp);
        logMsg.append("\n");
        logMsg.append("destPort: "+destPort);
        logMsg.append("\n");
        logMsg.append("realLength: "+realLength);
        logMsg.append("\n");
        logMsg.append("sequenceNumber: "+ sequenceNumber);
        logMsg.append("\n");
        logMsg.append("acknowledgmentNumber: "+acknowledgmentNumber);
        logMsg.append("\n");
        logMsg.append("---- parsed result ----");
        logMsg.append("\n");
        logMsg.append(message);
        logMsg.append("\n");
        logMsg.append("-------- end ---------\n");
        log.info(logMsg.toString());
    }

}
