package cn.rtm.protocol.parser;

import cn.rtm.protocol.parser.core.enums.ProtocolType;
import java.time.LocalDateTime;
import java.util.Map;

/**
 *  该类的作用主要是抽象 协议解析结果数据为公共的结构化数据内容
 *  与 数据库等协议解析结果一致，结构化存储
 */
public class ProtocolParseData {

    /**
     *  数据包唯一标识
     */
    private final long id;

    /**
     *  请求的来源 ip 地址
     */
    private final String srcIp;

    /**
     * 请求来源 ip 地址端口
     */
    private final int srcPort;

    /**
     *  请求的目标 ip 地址
     */
    private final String destIp;

    /**
     *  请求目标 ip 地址端口
     */
    private final int destPort;

    /**
     *  数据请求客户端标识
     */
    private final String clientId;

    /**
     *  请求数据标识语义，如关系数据库为 sql, 非关系数据库为 api 描述
     */
    private final String requestApi;

    /**
     *  协议类型，{@link ProtocolType}
     */
    private final String protocolType;

    /**
     *  存储请求数据提取的数据内容
     */
    private final String requestData;

    /**
     *  存储响应数据提取的数据内容
     */
    private final String responseData;

    /**
     * 响应数据包携带的响应体长度
     */
    private final int responseDataLength;

    /**
     *  请求数据包解析开始时间
     */
    private final LocalDateTime startTime;

    /**
     *  响应数据包解析开始时间
     */
    private final LocalDateTime endTime;

    /**
     *  提取数据执行时间
     */
    private final long executeTime;

    /**
     *  扩展字段，存储额外属性内容
     */
    private final Map<String,Object> extraValues;


    private ProtocolParseData(Builder builder) {
        this.id = builder.id;
        this.srcIp = builder.srcIp;
        this.srcPort = builder.srcPort;
        this.destIp = builder.destIp;
        this.destPort = builder.destPort;
        this.clientId = builder.clientId;
        this.requestApi = builder.requestApi;
        this.protocolType = builder.protocolType;
        this.requestData = builder.requestData;
        this.responseData = builder.responseData;
        this.responseDataLength = builder.responseDataLength;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
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

        private String requestData;

        private String responseData;

        private int responseDataLength;

        private LocalDateTime startTime;

        private LocalDateTime endTime;

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

        public Builder requestData(String requestData) {
            this.requestData = requestData;
            return this;
        }

        public Builder responseData(String responseData) {
            this.responseData = responseData;
            return this;
        }

        public Builder responseDataLength(int responseDataLength) {
            this.responseDataLength = responseDataLength;
            return this;
        }


        public Builder startTime(LocalDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(LocalDateTime endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder executeTime(long executeTime) {
            this.executeTime = executeTime;
            return this;
        }

        public Builder extraValues(Map<String,Object> extraValues) {
            this.extraValues = extraValues;
            return this;
        }

        public ProtocolParseData build() {
            return new ProtocolParseData(this);
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

    public String getRequestData() {
        return requestData;
    }

    public String getResponseData() {
        return responseData;
    }

    public int getResponseDataLength() {
        return responseDataLength;
    }


    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public Map<String, Object> getExtraValues() {
        return extraValues;
    }

    @Override
    public String toString() {
        return "ProtocolParseData{" +
                "id=" + id +
                ", srcIp='" + srcIp + '\'' +
                ", srcPort=" + srcPort +
                ", destIp='" + destIp + '\'' +
                ", destPort=" + destPort +
                ", clientId='" + clientId + '\'' +
                ", requestApi='" + requestApi + '\'' +
                ", protocolType='" + protocolType + '\'' +
                ", requestData='" + requestData + '\'' +
                ", responseData='" + responseData + '\'' +
                ", responseDataLength=" + responseDataLength +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", executeTime=" + executeTime +
                ", extraValues=" + extraValues +
                '}';
    }
}
