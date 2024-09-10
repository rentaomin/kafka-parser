package cn.rtm.kafkaParser.protocol.extractor;

import cn.rtm.kafkaParser.protocol.DataParseExtractor;
import cn.rtm.kafkaParser.protocol.ProtocolParseData;
import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.extractor.compose.CreateTopicsDataParseExtractor;
import cn.rtm.kafkaParser.protocol.extractor.compose.DeleteTopicsDataParseExtractor;
import cn.rtm.kafkaParser.protocol.extractor.compose.FetchDataParseExtractor;
import cn.rtm.kafkaParser.protocol.extractor.compose.ProduceDataParseExtractor;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiMessage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  该类的主要职责负责提供数据提取器工厂方法，自动注册获取实例
 */
public class DataParseExtractSupplier {

    /**
     *  存储协议解析结果数据提取实例，key: 协议解析结果类型，Value: 需要解析 Key 的数据提取器实例
     */
    public final static Map<Class<?>, DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>>> supplier = register();


    /**
     *  自动注册数据提取器实例
     * @return 返回数据提取器容器
     */
    private static Map<Class<?>, DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>>> register() {
        Map<Class<?>, DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>>> extractorMap = new ConcurrentHashMap<>(12);

        extractorMap.put(FetchRequestData.class, new FetchDataParseExtractor());
        extractorMap.put(FetchResponseData.class, new FetchDataParseExtractor());

        extractorMap.put(CreateTopicsRequestData.class, new CreateTopicsDataParseExtractor());
        extractorMap.put(CreateTopicsResponseData.class, new CreateTopicsDataParseExtractor());

        extractorMap.put(DeleteTopicsRequestData.class, new DeleteTopicsDataParseExtractor());
        extractorMap.put(DeleteTopicsResponseData.class, new DeleteTopicsDataParseExtractor());

        extractorMap.put(ProduceRequestData.class, new ProduceDataParseExtractor());
        extractorMap.put(ProduceResponseData.class, new ProduceDataParseExtractor());
        return extractorMap;
    }


    /**
     *  注册指定的数据提取器到容器中
     * @param clazz 待提取数据的数据类
     * @param dataParseExtractor 数据提取器
     */
    public void register(Class<?> clazz, DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>> dataParseExtractor) {
        supplier.put(clazz,dataParseExtractor);
    }


    /**
     *  获取 kafka 协议解析数据提取器
     * @param kafkaProtocolParsedMessage 请求-响应解析内容
     * @return 返回对应的数据提取器
     */
    public static DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>> getDataParseExtractor(KafkaProtocolParsedMessage kafkaProtocolParsedMessage) {
        if (kafkaProtocolParsedMessage == null) {
            return null;
        }
        ApiMessage requestMessage = kafkaProtocolParsedMessage.getRequestMessage();
        ApiMessage responseMessage = kafkaProtocolParsedMessage.getResponseMessage();
        // 全部解析完成才进行数据提取
        if (requestMessage == null || responseMessage == null) {
            return null;
        }
        Class<? extends ApiMessage> requestMessageClass = requestMessage.getClass();
        Class<? extends ApiMessage> responseMessageClass = responseMessage.getClass();
        return getDataParseExtractor(responseMessageClass);
    }


    /**
     *  获取指定数据类型对应的数据提取器
     * @param clazz 待提取数据的数据类
     * @return 返回支持提取的数据提取器
     */
    public static DataParseExtractor<KafkaProtocolParsedMessage,List<ProtocolParseData>> getDataParseExtractor(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }
        return supplier.get(clazz);
    }
}
