package cn.rtm.protocol.parser.protocols.kafka;

import cn.rtm.protocol.parser.*;
import cn.rtm.protocol.parser.protocols.kafka.factory.DataParseExtractSupplier;
import org.apache.commons.collections4.CollectionUtils;
import org.pcap4j.packet.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;


/**
 *  该类主要对外提供统一解析处理入口，负责管理 kafka 数据包解析，通过 {@link #handle(Packet)} 提供解析入口
 *  <ul>
 *  <li> 委托 {@link #packetReassemble} 实现数据包的重组，避免数据包不完整
 *  <li> 委托 {@link #requestParser} 实现请求数据包解析
 *  <li> 委托 {@link #responseParser} 实现响应数据包解析
 *  <li> 委托 {@link DataParseExtractor#extract(Object)} 实现完整数据包解析后数据内容提取
 *  <li> 请求和响应关系： a、请求的 seq + payLoadLength = 响应的 ack, 解析请求，存储key: seq+payLoadLength, value: 解析的请求内容，其中 payLoadLength=rawDataLength + 4;
 *  b、解析响应，先根据 ack 获取对应的请求内容，如果不存在，则跳过解析
 *  </ul>
 */
public class KafkaProtocolParseHandler implements ProtocolParseHandler<Packet, KafkaProtocolParsedMessage> {

    private Logger log = LoggerFactory.getLogger(getClass());

    /**
     *  默认 kafka 监听端口
     */
    private static final int DEFAULT_LISTEN_PORT = 9094;

    /**
     *  请求数据包解析器
     */
    private final ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> requestParser;

    /**
     *  响应数据包解析器
     */
    private final ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> responseParser;

    /**
     *  tcp 数据包重组组件，负责重组 tcp 分片包
     */
    private final PacketReassemble<ProtocolMessage> packetReassemble;

    /**
     *  数据解析提取器，对解析提取的数据内容做额外处理
     */
    private final DataParseExtractConsumer<List<ProtocolParseData>> dataParseExtractConsumer;

    /**
     *  需要解析协议的监听端口
     */
    private List<Integer> listenPorts;

    public KafkaProtocolParseHandler(
            PacketReassemble<ProtocolMessage> packetReassemble,
            ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> requestParser,
            ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> responseParser,
            DataParseExtractConsumer<List<ProtocolParseData>> dataParseExtractConsumer,
            List<Integer> listenPorts
            ) {
        this.packetReassemble = packetReassemble;
        this.requestParser = requestParser;
        this.responseParser = responseParser;
        this.dataParseExtractConsumer = dataParseExtractConsumer;
        this.listenPorts = listenPorts;
    }


    @Override
    public KafkaProtocolParsedMessage handle(Packet packet) {
        ProtocolMessage combinePacket = null;
        try {
            combinePacket = this.packetReassemble.reassemble(packet);
        } catch (Exception e) {
            log.error("重组数据包出错！", e);
        }

        if (combinePacket == null) {
            return null;
        }

        this.initializeListenPort(combinePacket);

        if (!combinePacket.isTargetPacket() || !combinePacket.isCompletePacket()) {
            return null;
        }

        KafkaProtocolParsedMessage kafkaProtocolParsedMessage = null;
        try {
            if (combinePacket.isRequestPacket()) {
                kafkaProtocolParsedMessage = this.requestParser.parse(combinePacket);
            } else if (combinePacket.isResponsePacket()){
                kafkaProtocolParsedMessage = responseParser.parse(combinePacket);
                DataParseExtractor<KafkaProtocolParsedMessage, List<ProtocolParseData>> dataParseExtractor = DataParseExtractSupplier.getDataParseExtractor(kafkaProtocolParsedMessage);
                if (dataParseExtractor == null) {
                    return kafkaProtocolParsedMessage;
                }
                this.dataParseExtractConsumer.accept(dataParseExtractor.extract(kafkaProtocolParsedMessage));
            }
        } catch (Exception e) {
            log.error("kafka 解析数据出错！", e);
        }
        return kafkaProtocolParsedMessage;
    }


    /**
     *   初始化需要解析的协议端口
     * @param combinePacket 捕获的完整数据包内容
     */
    private void initializeListenPort(ProtocolMessage combinePacket) {
        if (CollectionUtils.isEmpty(listenPorts)) {
            listenPorts = Arrays.asList(DEFAULT_LISTEN_PORT);
        }
        combinePacket.setListenPorts(listenPorts);
    }

}
