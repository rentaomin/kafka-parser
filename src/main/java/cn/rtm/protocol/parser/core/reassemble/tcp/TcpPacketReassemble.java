package cn.rtm.protocol.parser.core.reassemble.tcp;

import cn.rtm.protocol.parser.PacketReassemble;
import cn.rtm.protocol.parser.ProtocolMessage;
import cn.rtm.protocol.parser.core.util.ByteUtils;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  tcp 层 数据包分片重组实现，实现原理主要为：tcp 数据包传输，每个包都存在 sequenceNumber 和 acknowledgmentNumber ，用于确保
 *  数据包的顺序和标识分片和查找对应的请求-响应数据包；分片重组实现主要为
 *
 * <ul>
 * <li> 根据 {@linkplain #generatePacketKey(String, int, String, int, long)} 读取 {@linkplain #segmentBuffer} ，合并数据包
 * <li> 读取数据包真实大小，Kafka 数据包前四个字节为数据包真实大小 M
 * <li> 判断当前包的长度 - 4字节 是否等于数据包真实大小M,其中4个字节为数据包真实长度，若等于则时完整数据包直接处理
 * <li> 若不是完整数据包，则读取当前数据包的 acknowledgmentNumber,并通过 {@linkplain #generatePacketKey(String, int, String, int, long)} 作为 key，标识唯一数据包
 * value：为当前数据包，因为同一个请求分片包的 acknowledgmentNumber 始终一致，继续进行等待下个分片包
 * </ul>
 */
public class TcpPacketReassemble implements PacketReassemble<ProtocolMessage> {

    private final Logger log = LoggerFactory.getLogger(getClass());


    /**
     *  存储分片数据包内容，key: ack-id 标识 value: 对应的数据包
     */
    private final static Map<String, ByteBuffer> segmentBuffer = new ConcurrentHashMap<>(256);


    @Override
    public ProtocolMessage reassemble(Packet packet) {
        if (!packet.contains(TcpPacket.class)) {
            return null;
        }

        TcpPacket tcpPacket = packet.get(TcpPacket.class);
        if (tcpPacket == null) {
            return null;
        }

        Packet payloadPacket = tcpPacket.getPayload();
        // 跳过通信包
        if (payloadPacket == null) {
            return null;
        }

        TcpPacket.TcpHeader header = tcpPacket.getHeader();
        if (header == null) {
            return null;
        }

        int srcPort = header.getSrcPort().valueAsInt();
        int destPort = header.getDstPort().valueAsInt();

        IpV4Packet ipV4Packet = (IpV4Packet)packet.getPayload();
        String srcIp = ipV4Packet.getHeader().getSrcAddr().getHostAddress();
        String destIp = ipV4Packet.getHeader().getDstAddr().getHostAddress();
        long sequenceNumber = header.getSequenceNumberAsLong();
        long acknowledgmentNumber = header.getAcknowledgmentNumberAsLong();

        // 获取TCP载荷（即 Kafka 协议数据）
        byte[] payload = payloadPacket.getRawData();
        String packetKey = this.generatePacketKey(srcIp, srcPort, destIp, destPort, acknowledgmentNumber);

        ByteBuffer previousPacket = segmentBuffer.get(packetKey);

        ByteBuffer combinedPacket = ByteUtils.combineBuffers(previousPacket, ByteBuffer.wrap(payload));

        if (isCompletePacket(combinedPacket)) {
            segmentBuffer.remove(packetKey);
            return new ProtocolMessage(srcIp,srcPort,
                    destIp,destPort,sequenceNumber,acknowledgmentNumber,combinedPacket.array());
        } else {
            // 如果尚未接收完整，则缓存数据
            segmentBuffer.put(packetKey, combinedPacket);
        }
        return null;
    }


    /**
     *  生成数据包唯一标识
     * @param srcIp 请求 ip
     * @param srcPort 请求地址端口
     * @param destIp 目标地址 ip
     * @param destPort 目标地址端口
     * @param acknowledgmentNumber 数据包确认序号
     * @return 返回请求唯一标识
     */
    private String generatePacketKey(String srcIp, int srcPort, String destIp, int destPort, long acknowledgmentNumber) {
        return srcIp + ":" + srcPort + "-" + destIp + ":" + destPort + "-" + acknowledgmentNumber;
    }


    /**
     *  判断数据包是否为完整的数据包
     * @param buffer 数据包内容
     * @return 返回 true 则时完整数据包，反之 false
     */
    private boolean isCompletePacket(ByteBuffer buffer) {
        int totalPacketLength = getTotalPacketLength(buffer);
        int currentPacketLength = buffer.remaining();
        return (currentPacketLength - 4) == totalPacketLength;
    }


    /**
     *  获取包的总长度
     * @param buffer 数据包内容
     * @return 返回数据包真实长度
     */
    private int getTotalPacketLength(ByteBuffer buffer) {
        return buffer.slice().getInt(0);
    }

}
