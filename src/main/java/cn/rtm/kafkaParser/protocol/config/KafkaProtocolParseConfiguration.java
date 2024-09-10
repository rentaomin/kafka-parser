package cn.rtm.kafkaParser.protocol.config;

import cn.rtm.kafkaParser.protocol.*;
import cn.rtm.kafkaParser.protocol.consumer.KafkaDataParseExtractConsumer;
import cn.rtm.kafkaParser.protocol.extractor.DataParseExtractSupplier;
import cn.rtm.kafkaParser.protocol.parser.DefaultProtocolContext;
import cn.rtm.kafkaParser.protocol.parser.KafkaProtocolHandler;
import cn.rtm.kafkaParser.protocol.parser.req.KafkaRequestParser;
import cn.rtm.kafkaParser.protocol.parser.req.RequestParser;
import cn.rtm.kafkaParser.protocol.parser.res.KafkaResponseParser;
import cn.rtm.kafkaParser.protocol.parser.res.ResponseParser;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import java.util.List;

/**
 *  该配置类主要职责是声明 Kafka 协议解析所需要的 bean,支持灵活配置，若需要解析其他协议
 *  则通过协议前缀 bean 进行区分不同解析器
 */
@Configuration
public class KafkaProtocolParseConfiguration {

    @Bean
    @ConditionalOnMissingBean(TcpPacketCombiner.class)
    public PacketCombiner<ProtocolMessage> tcpPacketCombiner() {
        return new TcpPacketCombiner();
    }


    @Bean
    @ConditionalOnMissingBean(ProtocolContext.class)
    public ProtocolContext protocolContext() {
        return new DefaultProtocolContext();
    }


    @Bean
    @ConditionalOnMissingBean(KafkaRequestParser.class)
    public RequestParser<ProtocolMessage, Message> kafkaRequestParser(@Lazy ProtocolContext protocolContext) {
        return new KafkaRequestParser(protocolContext);
    }


    @Bean
    @ConditionalOnMissingBean(KafkaResponseParser.class)
    public ResponseParser<ProtocolMessage, Message> kafkaResponseParser(@Lazy ProtocolContext protocolContext) {
          return new KafkaResponseParser(protocolContext);
    }

    @Bean
    public DataParseExtractSupplier dataParseExtractSupplier() {
        return new DataParseExtractSupplier();
    }

    @Bean
    @ConditionalOnMissingBean(KafkaDataParseExtractConsumer.class)
    public DataParseExtractConsumer<List<KafkaData>> kafkaDataParseExtractConsumer() {
        return new KafkaDataParseExtractConsumer();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProtocolHandler kafkaProtocolHandler(PacketCombiner<ProtocolMessage> tcpPacketCombiner,
            RequestParser<ProtocolMessage, Message> kafkaRequestParser,
            ResponseParser<ProtocolMessage, Message> kafkaResponseParser,
            DataParseExtractConsumer<List<KafkaData>> kafkaDataParseExtractConsumer) {
            return new KafkaProtocolHandler(tcpPacketCombiner,kafkaRequestParser,
                    kafkaResponseParser,kafkaDataParseExtractConsumer);
    }
}
