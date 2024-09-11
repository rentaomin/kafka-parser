package cn.rtm.kafkaParser.protocol;

import cn.rtm.kafkaParser.protocol.util.ByteUtils;
import java.nio.ByteBuffer;
import java.util.List;


/**
 *  提供请求和响应数据包公共内容包装类
 */
public class ProtocolMessage {

    private String srcIp;

    private int srcPort;

    private String destIp;

    private int destPort;

    private long sequenceNumber;

    private long acknowledgementNumber;

    /**
     *  数据包的实际长度，未包含长度字节本身
     */
    private int length;

    /**
     *  kafka 协议原始数据
     */
    private byte[] rawData;

    /**
     *  默认协议监听端口，指定需要解析监听的协议端口
     */
    private List<Integer> listenPorts;

    public ProtocolMessage(String srcIp, int srcPort, String destIp, int destPort,
              long sequenceNumber, long acknowledgementNumber, byte[] rawData) {
        this.srcIp = srcIp;
        this.srcPort = srcPort;
        this.destIp = destIp;
        this.destPort = destPort;
        this.sequenceNumber = sequenceNumber;
        this.acknowledgementNumber = acknowledgementNumber;
        this.rawData = rawData;
        this.length = realPacketLength();
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(int srcPort) {
        this.srcPort = srcPort;
    }

    public String getDestIp() {
        return destIp;
    }

    public void setDestIp(String destIp) {
        this.destIp = destIp;
    }

    public int getDestPort() {
        return destPort;
    }

    public void setDestPort(int destPort) {
        this.destPort = destPort;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public long getAcknowledgementNumber() {
        return acknowledgementNumber;
    }

    public void setAcknowledgementNumber(long acknowledgementNumber) {
        this.acknowledgementNumber = acknowledgementNumber;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }

    public int getLength() {
        return this.length;
    }


    public ByteBuffer duplicateRawData() {
        return wrappedRawData().duplicate();
    }


    public ByteBuffer wrappedRawData() {
        return ByteUtils.wrap(this.rawData);
    }


    public List<Integer> getListenPorts() {
        return listenPorts;
    }

    public void setListenPorts(List<Integer> listenPorts) {
        this.listenPorts = listenPorts;
    }

    /**
     *  不包含数据包长度内容的数据包信息
     * @return
     */
    public ByteBuffer rawDataWithNoLength() {
        return ByteUtils.wrapWithNoLength(this.rawData);
    }


    /**
     *  获取传输包总大小, 前四个字节代表实际传输包的大小，由于网卡传输限制，超过则会进行 tcp 分片传输，该字段标识完整数据包大小
     * @return 返回请求或响应数据包真实的大小
     */
    public int realPacketLength(){
        return duplicateRawData().getInt();
    }


    /**
     *  获取当前传输包大小
     * @return 返回当前请求携带的数据包的大小
     */
    public int currentPacketLength() {
        return wrappedRawData().remaining();
    }


    /**
     *  判断数据包是否为完整的数据包，读取数据包前4个字节获取总数据包长度，并与当前数据包的长度进行比较，
     *  若相等则返回 true，反之则返回 false；若为分片数据包，则总长度为 0，则返回 false，反之则返回 true
     * @return 返回 true 则是，反之 false
     */
    public boolean isCompletePacket() {
        return this.length  == (currentPacketLength() - 4) ;
    }


    /**
     *  判断请求的目标端口是否为 kafka 协议配置的端口
     * @return 返回 true 则是，反之 false
     */
    public boolean isRequestPacket() {
        return this.listenPorts.contains(this.destPort);
    }


    /**
     *  判断请求的请求来源端口是否为 kafka 协议配置的端口
     * @return 返回 true 则是，反之 false
     */
    public boolean isResponsePacket() {
        return this.listenPorts.contains(this.srcPort);
    }


    /**
     * 是否为需要解析的目标协议包，即非空数据包且为请求数据包或响应数据包
     * @return 返回 true 则是，反之false
     */
    public boolean isTargetPacket() {
        if (isEmptyPacket()) {
            return false;
        }
        return isRequestPacket() || isResponsePacket();
    }


    /**
     *  是否为空数据包
     * @return 返回 true 则是，反之 false
     */
    public boolean isEmptyPacket() {
        return this.rawData.length == 0;
    }


    /**
     *  请求标识，获取当前请求对应的响应数据包 ack-id ,用于关联一组请求和响应数据包
     * @return 返回该请求对应的响应数据包 ack-id
     */
    public long getResponseAckId() {
        return getSequenceNumber() + this.length + 4;
    }


    /**
     *  当前数据包请求地址描述信息
     * @return 返回数据包请求概览信息
     */
    public String requestDesc() {
        StringBuilder frame = new StringBuilder(32);
        frame.append("--------- req -> res ---------\n");
        frame.append("sequenceNumber: ");
        frame.append(this.sequenceNumber);
        frame.append("\n");
        frame.append("acknowledgementNumber: ");
        frame.append(this.acknowledgementNumber);
        frame.append("\n");
        frame.append(this.srcIp);
        frame.append(":");
        frame.append(this.srcPort);
        frame.append("=>");
        frame.append(this.destIp);
        frame.append(":");
        frame.append(this.destPort);
        frame.append("\n");
        frame.append("--------- req -> res ---------\n");
        return frame.toString();
    }

    public String getRequestUrl() {
        return this.srcIp + ":" +this.srcPort + " <-> " + this.destIp + ":" + this.destPort;
    }

    @Override
    public String toString() {
        return "ProtocolMessage{" +
                "srcIp='" + srcIp + '\'' +
                ", srcPort=" + srcPort +
                ", destIp='" + destIp + '\'' +
                ", destPort=" + destPort +
                ", sequenceNumber=" + sequenceNumber +
                ", acknowledgementNumber=" + acknowledgementNumber +
                ", length=" + length +
                ", listenPorts=" + listenPorts +
                '}';
    }
}
