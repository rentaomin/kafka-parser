package cn.rtm.protocol.parser.protocols.kafka.autoconfig;

import cn.rtm.protocol.parser.*;
import cn.rtm.protocol.parser.protocols.kafka.consumer.KafkaDataParseExtractConsumer;
import cn.rtm.protocol.parser.protocols.kafka.factory.DataParseExtractSupplier;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParseHandler;
import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParsedMessage;
import cn.rtm.protocol.parser.core.context.ProtocolParseContext;
import cn.rtm.protocol.parser.protocols.kafka.parser.request.KafkaRequestParser;
import cn.rtm.protocol.parser.protocols.kafka.parser.response.KafkaResponseBodyParser;
import cn.rtm.protocol.parser.core.reassemble.tcp.TcpPacketReassemble;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.Arrays;
import java.util.List;

/**
 *  该配置类主要职责是声明 Kafka 协议解析所需要的 bean,支持灵活配置，若需要解析其他协议
 *  则通过协议前缀 bean 进行区分不同解析器
 */
@Configuration
public class KafkaProtocolParseConfiguration {

    @Bean
    @ConditionalOnMissingBean(TcpPacketReassemble.class)
    public PacketReassemble<ProtocolMessage> tcpPacketCombiner() {
        return new TcpPacketReassemble();
    }


    @Bean
    @ConditionalOnMissingBean(ProtocolContext.class)
    public ProtocolContext protocolContext() {
        return new ProtocolParseContext();
    }


    @Bean
    @ConditionalOnMissingBean(KafkaRequestParser.class)
    public ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> kafkaRequestParser(ProtocolContext protocolContext) {
        return new KafkaRequestParser(protocolContext);
    }


    @Bean
    @ConditionalOnMissingBean(KafkaResponseBodyParser.class)
    public ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> kafkaResponseParser(ProtocolContext protocolContext) {
          return new KafkaResponseBodyParser(protocolContext);
    }

    @Bean
    public DataParseExtractSupplier dataParseExtractSupplier() {
        return new DataParseExtractSupplier();
    }

    @Bean
    @ConditionalOnMissingBean(KafkaDataParseExtractConsumer.class)
    public DataParseExtractConsumer<List<ProtocolParseData>> kafkaDataParseExtractConsumer() {
        return new KafkaDataParseExtractConsumer();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProtocolParseHandler kafkaProtocolHandler(PacketReassemble<ProtocolMessage> tcpPacketReassemble,
                                                     ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> kafkaRequestParser,
                                                     ProtocolParser<ProtocolMessage, KafkaProtocolParsedMessage> kafkaResponseParser,
                                                     DataParseExtractConsumer<List<ProtocolParseData>> kafkaDataParseExtractConsumer) {
            return new KafkaProtocolParseHandler(tcpPacketReassemble,kafkaRequestParser,
                    kafkaResponseParser,kafkaDataParseExtractConsumer, Arrays.asList(9094));
    }
}
