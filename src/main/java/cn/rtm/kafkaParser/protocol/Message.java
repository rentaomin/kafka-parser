package cn.rtm.kafkaParser.protocol;


import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestHeader;

/**
 *  Kafka 消息体
 *
 *  RequestOrResponse => Size (RequestMessage | ResponseMessage)
 *   Size => int32
 * @author rtm
 */
public class Message {

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
     *  标记一组 请求-响应 数据包解析完成
     */
    private boolean parseComplete = false;


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


    public RequestHeader getRequestHeader() {
        return requestHeader;
    }


    public void setRequestHeader(RequestHeader requestHeader) {
        this.requestHeader = requestHeader;
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


    @Override
    public String toString() {
        return "Message{" +
                "messageSize=" + messageSize +
                ", requestLength=" + requestLength +
                ", responseLength=" + responseLength +
                ", requestMessage=" + requestMessage +
                ", responseMessage=" + responseMessage +
                ", parseComplete=" + parseComplete +
                '}';
    }
}
