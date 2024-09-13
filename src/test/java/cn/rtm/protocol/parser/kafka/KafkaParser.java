package cn.rtm.protocol.parser.kafka;


import cn.rtm.protocol.parser.*;
import cn.rtm.protocol.parser.protocols.kafka.consumer.KafkaDataParseExtractConsumer;
import cn.rtm.protocol.parser.ProtocolParseException;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParseHandler;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParsedMessage;
import cn.rtm.protocol.parser.core.context.ProtocolParseContext;
import cn.rtm.protocol.parser.protocols.kafka.parser.request.KafkaRequestParser;
import cn.rtm.protocol.parser.protocols.kafka.parser.response.KafkaResponseBodyParser;
import cn.rtm.protocol.parser.core.reassemble.tcp.TcpPacketReassemble;
import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;
import java.util.Arrays;
import java.util.List;


/**
 *
 *   RequestOrResponse => Size (RequestMessage | ResponseMessage)
 *   Size => int32
 *  1、区分是什么协议，通过 ip:port 端口进行区分
 *  2、区分是请求还是响应；观察流量请求包和响应包，发现区分请求中是否包含 clientId：NULLABLE_STRING (INT16) < 0 ? -1 : N
 *  3、解析公共数据包，跳过空包
 */
public class KafkaParser {

   static short LISTEN_PORT = 9094;


    public static void main(String[] args) throws PcapNativeException, NotOpenException, ProtocolParseException {
        // 获取所有网络接口
        List<PcapNetworkInterface> allDevs = Pcaps.findAllDevs();
        if (allDevs.isEmpty()) {
            System.out.println("没有找到网络接口.");
            return;
        }

        // 选择第一个网络接口
        PcapNetworkInterface nif = allDevs.get(3);

        KafkaParser kafkaParser = new KafkaParser();

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
                    kafkaParser.parsePacket(packet);
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
        ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> requestParser = new KafkaRequestParser(protocolContext);
        ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> responseParser = new KafkaResponseBodyParser(protocolContext);
        DataParseExtractConsumer<List<ProtocolParseData>> kafkaDataParseExtractConsumer = new KafkaDataParseExtractConsumer();
        ProtocolParseHandler<Packet, KafkaProtocolParsedMessage> protocolParseHandler = new KafkaProtocolParseHandler(packetReassemble,requestParser,
                responseParser,kafkaDataParseExtractConsumer, Arrays.asList(9094));
        return protocolParseHandler;
    }



}
