package cn.rtm.kafkaParser.protocol;

import cn.rtm.kafkaParser.protocol.enums.ProtocolType;
import java.util.Map;

/**
 *  该类的作用主要是抽象 kafka 解析结果数据为公共的结构化数据内容
 *  与 数据库等协议解析结果一致，结构化存储
 */
public class KafkaData{

    private long id;

    private String srcIp;

    private int srcPort;

    private String destIp;

    private int destPort;

    private String clientId;

    private String requestApi;

    /**
     *  协议类型
     */
    private String protocolType;

    private String requestTopic;

    private String responseRecord;

    private int responseDataLength;

    private long executeTime;

    private Map<String,Object> extraValues;


    private KafkaData(Builder builder) {
        this.id = builder.id;
        this.srcIp = builder.srcIp;
        this.srcPort = builder.srcPort;
        this.destIp = builder.destIp;
        this.destPort = builder.destPort;
        this.clientId = builder.clientId;
        this.requestApi = builder.requestApi;
        this.protocolType = builder.protocolType;
        this.requestTopic = builder.requestTopic;
        this.responseRecord = builder.responseRecord;
        this.responseDataLength = builder.responseDataLength;
        this.executeTime = builder.executeTime;
        this.extraValues = builder.extraValues;
    }

    public static class Builder {
        private long id;

        private String srcIp;

        private int srcPort;

        private String destIp;

        private int destPort;

        private String clientId;

        private String requestApi;

        /**
         *  协议类型
         */
        private String protocolType = ProtocolType.KAFKA.getName();

        private String requestTopic;

        private String responseRecord;

        private int responseDataLength;

        private long executeTime;

        private Map<String,Object> extraValues;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder srcIp(String srcIp) {
            this.srcIp = srcIp;
            return this;
        }

        public Builder srcPort(int srcPort) {
            this.srcPort = srcPort;
            return this;
        }

        public Builder destIp(String destIp) {
            this.destIp = destIp;
            return this;
        }

        public Builder destPort(int destPort) {
            this.destPort = destPort;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder requestApi(String requestApi) {
            this.requestApi = requestApi;
            return this;
        }

        public Builder protocolType(String protocolType) {
            this.protocolType = protocolType;
            return this;
        }

        public Builder requestTopic(String requestTopic) {
            this.requestTopic = requestTopic;
            return this;
        }

        public Builder responseRecord(String responseRecord) {
            this.responseRecord = responseRecord;
            return this;
        }

        public Builder responseDataLength(int responseDataLength) {
            this.responseDataLength = responseDataLength;
            return this;
        }

        public Builder executeTime(long executeTime) {
            this.executeTime = executeTime;
            return this;
        }

        public Builder extraValues(Map<String,Object> executeTime) {
            this.extraValues = extraValues;
            return this;
        }

        public KafkaData build() {
            return new KafkaData(this);
        }
    }

    public long getId() {
        return id;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public String getDestIp() {
        return destIp;
    }

    public int getDestPort() {
        return destPort;
    }

    public String getProtocolType() {
        return protocolType;
    }


    public String getClientId() {
        return clientId;
    }

    public String getRequestApi() {
        return requestApi;
    }

    public String getRequestTopic() {
        return requestTopic;
    }

    public String getResponseRecord() {
        return responseRecord;
    }

    public int getResponseDataLength() {
        return responseDataLength;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public Map<String, Object> getExtraValues() {
        return extraValues;
    }

    @Override
    public String toString() {
        return "KafkaData{" +
                "id=" + id +
                ", srcIp='" + srcIp + '\'' +
                ", srcPort=" + srcPort +
                ", destIp='" + destIp + '\'' +
                ", destPort=" + destPort +
                ", clientId='" + clientId + '\'' +
                ", requestApi='" + requestApi + '\'' +
                ", protocolType='" + protocolType + '\'' +
                ", requestTopic='" + requestTopic + '\'' +
                ", responseRecord='" + responseRecord + '\'' +
                ", responseDataLength=" + responseDataLength +
                ", executeTime=" + executeTime +
                ", extraValues=" + extraValues +
                '}';
    }
}
