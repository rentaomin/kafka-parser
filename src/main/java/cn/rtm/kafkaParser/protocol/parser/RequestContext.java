package cn.rtm.kafkaParser.protocol.parser;

import cn.rtm.kafkaParser.protocol.InFlightRequests;
import cn.rtm.kafkaParser.protocol.Message;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RequestContext {

    private RequestHeader requestHeader;

    private AbstractRequest requestBody;

    private InFlightRequests requestCache = new InFlightRequests();

    private final Map<Integer, Message> res = new ConcurrentHashMap<>(16);


    public void add(String node, RequestHeader requestHeader) {
        requestCache.add(node,requestHeader);
    }

    public RequestHeader get(String node) {
       return requestCache.completeNext(node);
    }

    public RequestHeader getRequestHeader() {
        return requestHeader;
    }

    public void setRequestHeader(RequestHeader requestHeader) {
        this.requestHeader = requestHeader;
    }

    public AbstractRequest getRequestBody() {
        return requestBody;
    }

    public void setRequestBody(AbstractRequest requestBody) {
        this.requestBody = requestBody;
    }

    public void putRequestMsg(int requestId, ApiMessage requestMessage) {
        Message msg = res.get(requestId);
        if (msg == null) {
            msg = new Message();
        }
        msg.setRequestMessage(requestMessage);
        res.put(requestId,msg);
    }

    public void putRequestMsgLength(int requestId, int length) {
        Message msg = res.get(requestId);
        if (msg == null) {
            msg = new Message();
        }
        msg.setRequestLength(length);
        res.put(requestId, msg);
    }

    public void putResponseMsg(int requestId, ApiMessage responseMessage) {
        Message msg = res.get(requestId);
        if (msg == null) {
            msg = new Message();
        }
        msg.setResponseMessage(responseMessage);
        msg.setParseComplete(Boolean.TRUE);
        res.put(requestId,msg);
    }

    public void putResponseMsgLength(int requestId, int length) {
        Message msg = res.get(requestId);
        if (msg == null) {
            msg = new Message();
        }
        msg.setResponseLength(length);
        res.put(requestId, msg);
    }



    public ApiMessage getRequestMessage(int requestId) {
        return getMessage(requestId).getRequestMessage();
    }


    public Message getMessage(int requestId) {
        return res.get(requestId);
    }


    public Message buildParsedMessage(int requestId, ApiMessage responseMessage) {
        Message msg = getMessage(requestId);
        if (msg == null) {
            msg = new Message();
        }
        msg.setResponseMessage(responseMessage);
        msg.setParseComplete(Boolean.TRUE);

        removeMessage(requestId);
        return msg;
    }

    public void removeMessage(int requestId) {
        res.remove(requestId);
    }

}
