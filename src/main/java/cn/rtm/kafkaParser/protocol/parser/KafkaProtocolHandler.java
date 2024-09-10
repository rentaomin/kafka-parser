package cn.rtm.kafkaParser.protocol.parser;

import cn.rtm.kafkaParser.protocol.*;
import cn.rtm.kafkaParser.protocol.extractor.DataParseExtractSupplier;
import cn.rtm.kafkaParser.protocol.parser.req.RequestParser;
import cn.rtm.kafkaParser.protocol.parser.res.ResponseParser;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *  该类主要对外提供统一解析处理入口，负责管理 kafka 数据包解析，通过 {@link #handle(Packet)} 提供解析入口
 *  <ul>
 *  <li> 委托 {@link #packetCombiner} 实现数据包的重组，避免数据包不完整
 *  <li> 委托 {@link #requestParser} 实现请求数据包解析
 *  <li> 委托 {@link #responseParser} 实现响应数据包解析
 *  <li> 委托 {@link DataParseExtractor#extract(Object)} 实现完整数据包解析后数据内容提取
 *  <li> 请求和响应关系： a、请求的 seq + payLoadLength = 响应的 ack, 解析请求，存储key: seq+payLoadLength, value: 解析的请求内容，其中 payLoadLength=rawDataLength + 4;
 *  b、解析响应，先根据 ack 获取对应的请求内容，如果不存在，则跳过解析
 *  </ul>
 */
public class KafkaProtocolHandler implements ProtocolHandler<Packet, Message> {

    private Logger log = LoggerFactory.getLogger(getClass());

    private final RequestParser<ProtocolMessage, Message> requestParser;
    private final ResponseParser<ProtocolMessage, Message> responseParser;
    private final PacketCombiner<ProtocolMessage> packetCombiner;

    private final DataParseExtractConsumer<List<KafkaData>> dataParseExtractConsumer;

    public KafkaProtocolHandler(
            PacketCombiner<ProtocolMessage> packetCombiner,
            RequestParser<ProtocolMessage, Message> requestParser,
            ResponseParser<ProtocolMessage, Message> responseParser,
            DataParseExtractConsumer<List<KafkaData>> dataParseExtractConsumer
            ) {
        this.packetCombiner = packetCombiner;
        this.requestParser = requestParser;
        this.responseParser = responseParser;
        this.dataParseExtractConsumer = dataParseExtractConsumer;
    }


    @Override
    public Message handle(Packet packet) {
        ProtocolMessage combinePacket = null;
        try {
            combinePacket = this.packetCombiner.combine(packet);
        } catch (Exception e) {
            log.error("重组数据包出错！", e);
        }
        if (combinePacket == null || !combinePacket.isKafkaPacket() || !combinePacket.isCompletePacket()) {
            return null;
        }
        Message message = null;
        try {
            if (combinePacket.isRequestPacket()) {
                message = this.requestParser.parse(combinePacket);
            } else {
                message = responseParser.parse(combinePacket);
                DataParseExtractor<Message, List<KafkaData>> dataParseExtractor = DataParseExtractSupplier.getDataParseExtractor(message);
                if (dataParseExtractor == null) {
                    return message;
                }
                this.dataParseExtractConsumer.accept(dataParseExtractor.extract(message));
            }
        } catch (Exception e) {
            log.error("kafka 解析数据出错！", e);
        }
        return message;
    }

}
