package cn.rtm.protocol.parser.protocols.redis;

import cn.rtm.protocol.parser.PacketReassemble;
import cn.rtm.protocol.parser.ProtocolMessage;
import cn.rtm.protocol.parser.ProtocolParseHandler;
import cn.rtm.protocol.parser.ProtocolParser;
import org.apache.commons.collections4.CollectionUtils;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class RedisProtocolParseHandler implements ProtocolParseHandler<Packet,RedisParsedMessage> {

    private Logger log = LoggerFactory.getLogger(getClass());

    /**
     *  默认 kafka 监听端口
     */
    private static final int DEFAULT_LISTEN_PORT = 6379;

    /**
     *  tcp 数据包重组组件，负责重组 tcp 分片包
     */
    private final PacketReassemble<ProtocolMessage> packetReassemble;

    private final ProtocolParser<ProtocolMessage, RedisParseData> requestParser;

    private final ProtocolParser<ProtocolMessage, RedisParseData> responseParser;

    /**
     *  需要解析协议的监听端口
     */
    private List<Integer> listenPorts;

    public RedisProtocolParseHandler(PacketReassemble<ProtocolMessage> packetReassemble,
             ProtocolParser<ProtocolMessage, RedisParseData> requestParser,
             ProtocolParser<ProtocolMessage, RedisParseData> responseParser,
             List<Integer> listenPorts) {
        this.packetReassemble = packetReassemble;
        this.requestParser = requestParser;
        this.responseParser = responseParser;
        this.listenPorts = listenPorts;
    }

    @Override
    public RedisParsedMessage handle(Packet packet) {
        ProtocolMessage combinePacket = packetReassemble.reassemble(packet);
        if (combinePacket == null) {
            return null;
        }

        this.initializeListenPort(combinePacket);

        if (!combinePacket.isTargetPacket() || !combinePacket.isCompletePacket()) {
            return null;
        }

        RedisParseData redisParseData = null;
        try {
            if (combinePacket.isRequestPacket()) {
                redisParseData = this.requestParser.parse(combinePacket);
            } else if (combinePacket.isResponsePacket()){
                redisParseData = responseParser.parse(combinePacket);
            }
        } catch (Exception e) {
            log.error("kafka 解析数据出错！", e);
        }
        return null;
    }

    private void initializeListenPort(ProtocolMessage combinePacket) {
        if (CollectionUtils.isEmpty(listenPorts)) {
            listenPorts = Arrays.asList(DEFAULT_LISTEN_PORT);
        }
        combinePacket.setListenPorts(listenPorts);
    }
}
