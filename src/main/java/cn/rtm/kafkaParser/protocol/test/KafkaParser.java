package cn.rtm.kafkaParser.protocol.test;


import cn.rtm.kafkaParser.protocol.*;
import cn.rtm.kafkaParser.protocol.consumer.KafkaDataParseExtractConsumer;
import cn.rtm.kafkaParser.protocol.exception.ProtocolParseException;
import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.parser.DefaultProtocolContext;
import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolHandler;
import cn.rtm.kafkaParser.protocol.parser.req.KafkaRequestParser;
import cn.rtm.kafkaParser.protocol.parser.req.RequestParser;
import cn.rtm.kafkaParser.protocol.parser.res.KafkaResponseParser;
import cn.rtm.kafkaParser.protocol.parser.res.ResponseParser;
import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;
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


    private ProtocolHandler protocolHandler = getProtocolParseCoordinator();

    // 解析 MySQL 数据包
    private void parsePacket(Packet packet) {
        protocolHandler.handle(packet);
    }

    private ProtocolHandler getProtocolParseCoordinator() {
        PacketCombiner<ProtocolMessage> packetCombiner = new TcpPacketCombiner();
        ProtocolContext protocolContext = new DefaultProtocolContext();
        RequestParser<ProtocolMessage, KafkaProtocolParsedMessage> requestParser = new KafkaRequestParser(protocolContext);
        ResponseParser<ProtocolMessage, KafkaProtocolParsedMessage> responseParser = new KafkaResponseParser(protocolContext);
        DataParseExtractConsumer<List<ProtocolParseData>> kafkaDataParseExtractConsumer = new KafkaDataParseExtractConsumer();
        ProtocolHandler<Packet, KafkaProtocolParsedMessage> protocolHandler = new KafkaProtocolHandler(packetCombiner,requestParser,
                responseParser,kafkaDataParseExtractConsumer);
        return protocolHandler;
    }



}
