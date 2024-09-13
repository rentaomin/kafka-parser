package cn.rtm.protocol.parser.protocols.kafka;


import cn.rtm.protocol.parser.ProtocolMessage;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestHeader;
import java.time.LocalDateTime;

/**
 *  该类主要负责存储 Kafka 协议解析结果
 *
 *  RequestOrResponse => Size (RequestMessage | ResponseMessage)
 *   Size => int32
 * @author rtm
 */
public class KafkaProtocolParsedMessage {

    /**
     * 	The message_size field gives the size of the subsequent request or response message in bytes.
     * 	The client can read requests by first reading this 4 byte size as an integer N, and then
     * 	reading and parsing the subsequent N bytes of the request.
     * 	Size => int32
     */
    private int messageSize;

    /**
     *  请求数据包 payload 大小
     */
    private int requestLength;

    /**
     *  响应数据包 payload 大小
     */
    private int responseLength;

    /**
     *  请求 api 描述信息
     */
    private String requestApi;

    private ProtocolMessage originData;

    /**
     *  存储请求数据包 header 解析完成的内容
     */
    private RequestHeader requestHeader;

    /**
     * 存储响应数据包 header 解析完成的内容
     */
    private ResponseHeaderData responseHeader;

    /**
     *  存储请求数据包 payload 解析完成的内容
     */
    private ApiMessage requestMessage;

    /**
     *  存储响应数据包 payload 解析完成的内容
     */
    private ApiMessage responseMessage;

    /**
     *  标记请求响应数据包是否解析完成
     */
    private boolean parsedRequest;

    /**
     *  标记响应数据包，是否解析完成
     */
    private boolean parsedResponse;

    /**
     *  标记一组 请求-响应 数据包解析完成
     */
    private boolean parseComplete ;

    /**
     *  标记是否为请求数据包，true 则为 请求数据解析内容，false 则为响应数据内容
     */
    private boolean isRequestData;

    /**
     *  请求数据包解析开始时间
     */
    private LocalDateTime startTime;

    /**
     *  响应数据包解析结束时间
     */
    private LocalDateTime endTime;


    public int getMessageSize() {
        return messageSize;
    }


    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }


    public int getRequestLength() {
        return requestLength;
    }


    public void setRequestLength(int requestLength) {
        this.requestLength = requestLength;
    }


    public int getResponseLength() {
        return responseLength;
    }


    public void setResponseLength(int responseLength) {
        this.responseLength = responseLength;
    }

    public ProtocolMessage getOriginData() {
        return originData;
    }

    public void setOriginData(ProtocolMessage originData) {
        this.originData = originData;
    }

    public RequestHeader getRequestHeader() {
        return requestHeader;
    }


    public void setRequestHeader(RequestHeader requestHeader) {
        this.requestHeader = requestHeader;
    }

    public String getRequestApi() {
        return requestApi;
    }

    public void setRequestApi(String requestApi) {
        this.requestApi = requestApi;
    }

    public ResponseHeaderData getResponseHeader() {
        return responseHeader;
    }


    public void setResponseHeader(ResponseHeaderData responseHeader) {
        this.responseHeader = responseHeader;
    }


    public ApiMessage getRequestMessage() {
        return requestMessage;
    }


    public void setRequestMessage(ApiMessage requestMessage) {
        this.requestMessage = requestMessage;
    }


    public ApiMessage getResponseMessage() {
        return responseMessage;
    }


    public void setResponseMessage(ApiMessage responseMessage) {
        this.responseMessage = responseMessage;
    }


    public boolean isParsedRequest() {
        return parsedRequest;
    }

    public void setParsedRequest(boolean parsedRequest) {
        this.parsedRequest = parsedRequest;
    }

    public boolean isParsedResponse() {
        return parsedResponse;
    }

    public void setParsedResponse(boolean parsedResponse) {
        this.parsedResponse = parsedResponse;
    }

    /**
     *  判断当据包是否解析完成，只有一组请求包和响应包都解析完成才会认为解析完成
     * @return 返回 true 则解析完成， 反之 false
     */
    public boolean isParseComplete() {
        return parseComplete;
    }


    /**
     *  当请求数据包解析完成，且对应的响应数据包也解析完成，则更新解析状态为解析完成
     * @param parseComplete 解析完成状态
     */
    public void setParseComplete(boolean parseComplete) {
        this.parseComplete = parseComplete;
    }


    public boolean isRequestData() {
        return isRequestData;
    }

    public void setRequestData(boolean requestData) {
        isRequestData = requestData;
    }


    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "KafkaProtocolParsedMessage{" +
                "messageSize=" + messageSize +
                ", requestLength=" + requestLength +
                ", responseLength=" + responseLength +
                ", requestApi='" + requestApi + '\'' +
                ", originData=" + originData +
                ", requestHeader=" + requestHeader +
                ", responseHeader=" + responseHeader +
                ", requestMessage=" + requestMessage +
                ", responseMessage=" + responseMessage +
                ", parsedRequest=" + parsedRequest +
                ", parsedResponse=" + parsedResponse +
                ", parseComplete=" + parseComplete +
                ", isRequestData=" + isRequestData +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
