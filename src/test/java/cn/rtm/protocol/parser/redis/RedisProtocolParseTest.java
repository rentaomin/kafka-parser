package cn.rtm.protocol.parser.redis;

import cn.rtm.protocol.parser.*;
import cn.rtm.protocol.parser.core.context.ProtocolParseContext;
import cn.rtm.protocol.parser.core.reassemble.tcp.TcpPacketReassemble;
import cn.rtm.protocol.parser.protocols.redis.RedisParseData;
import cn.rtm.protocol.parser.protocols.redis.RedisParsedMessage;
import cn.rtm.protocol.parser.protocols.redis.RedisProtocolParseHandler;
import cn.rtm.protocol.parser.protocols.redis.parser.request.RedisRequestParser;
import cn.rtm.protocol.parser.protocols.redis.parser.response.RedisResponseParser;
import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;

import java.util.Arrays;
import java.util.List;

public class RedisProtocolParseTest {

    static short LISTEN_PORT = 6379;
    public static void main(String[] args) throws PcapNativeException, NotOpenException {
        // 获取所有网络接口
        List<PcapNetworkInterface> allDevs = Pcaps.findAllDevs();
        if (allDevs.isEmpty()) {
            System.out.println("没有找到网络接口.");
            return;
        }

        // 选择第一个网络接口
        PcapNetworkInterface nif = allDevs.get(3);

        RedisProtocolParseTest redisProtocolParseTest = new RedisProtocolParseTest();

        // 打开网络接口
        int snapLen = 65536;           // 捕获的最大数据包大小
        int timeout = 10 * 1000;       // 超时时间（毫秒）
        PcapHandle handle = nif.openLive(snapLen, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, timeout);
        // 设置过滤器，只捕获发往MySQL默认端口（3306）的TCP流量
        handle.setFilter("tcp port "+String.valueOf(LISTEN_PORT), BpfProgram.BpfCompileMode.OPTIMIZE);
        // 捕获数据包
        try {
            while (true) {
                Packet packet = null;
                try {
                    packet = handle.getNextPacketEx();
                } catch (Exception e) {
                    System.out.println("异常");
                }
                if (packet != null) {
                    redisProtocolParseTest.parsePacket(packet);
                }
            }
        } catch (Exception e) {
            System.out.println("捕获结束.");
        } finally {
            handle.close();
        }
    }

    private ProtocolParseHandler protocolParseHandler = getProtocolParseCoordinator();

    // 解析 MySQL 数据包
    private void parsePacket(Packet packet) {
        protocolParseHandler.handle(packet);
    }

    private ProtocolParseHandler getProtocolParseCoordinator() {
        PacketReassemble<ProtocolMessage> packetReassemble = new TcpPacketReassemble();
        ProtocolContext protocolContext = new ProtocolParseContext();
        ProtocolParser<ProtocolMessage, RedisParseData> requestParser = new RedisRequestParser(protocolContext);
        ProtocolParser<ProtocolMessage, RedisParseData> responseParser = new RedisResponseParser(protocolContext);
        ProtocolParseHandler<Packet, RedisParsedMessage> protocolParseHandler = new RedisProtocolParseHandler(packetReassemble,
                requestParser, responseParser,Arrays.asList(9094));
        return protocolParseHandler;
    }

}
