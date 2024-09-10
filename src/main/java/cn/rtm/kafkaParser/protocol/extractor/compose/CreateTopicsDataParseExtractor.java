package cn.rtm.kafkaParser.protocol.extractor.compose;

import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.ProtocolParseData;
import cn.rtm.kafkaParser.protocol.extractor.AbstractDataParseExtractor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 *  负责提取 Kafka-CreateTopics 请求解析内容 {@linkplain  org.apache.kafka.common.message.CreateTopicsRequestData }和
 *  响应解析内容 {@linkplain  org.apache.kafka.common.message.CreateTopicsResponseData}
 */
public class CreateTopicsDataParseExtractor extends AbstractDataParseExtractor<CreateTopicsRequestData,CreateTopicsResponseData,List<String>,List<String>> {

    public CreateTopicsDataParseExtractor() {
        super(CreateTopicsRequestData.class, CreateTopicsResponseData.class);
    }


    @Override
    protected List<String> extractRequest(CreateTopicsRequestData createTopicsRequestData) {
        if (createTopicsRequestData == null) {
            return null;
        }
        CreateTopicsRequestData.CreatableTopicCollection topics = createTopicsRequestData.topics();
        if (topics == null) {
            return null;
        }
        return topics.stream()
                .map(CreateTopicsRequestData.CreatableTopic::name)
                .collect(Collectors.toList());
    }


    @Override
    protected List<String> extractResponse(CreateTopicsResponseData createTopicsResponseData) {
        CreateTopicsResponseData.CreatableTopicResultCollection topics = createTopicsResponseData.topics();
        if (topics == null) {
            return null;
        }
        return topics.stream()
                .map(CreateTopicsResponseData.CreatableTopicResult::name)
                .collect(Collectors.toList());
    }


    @Override
    protected List<ProtocolParseData> composeData(KafkaProtocolParsedMessage kafkaProtocolParsedMessage, List<String> requestData, List<String> responseRecord) {
        if (CollectionUtils.isEmpty(requestData)) {
            return Collections.emptyList();
        }
        List<ProtocolParseData> data = new ArrayList<>();

        Map<String, String> recordValues = responseRecord.stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));

        ProtocolMessage originData = getOriginData();

        for (String topicName : requestData) {
            String resTopic = recordValues.get(topicName);
            ProtocolParseData protocolParseData = new ProtocolParseData.Builder()
                    .srcIp(originData.getSrcIp())
                    .srcPort(originData.getSrcPort())
                    .destIp(originData.getDestIp())
                    .destPort(originData.getDestPort())
                    .clientId(kafkaProtocolParsedMessage.getRequestHeader().clientId())
                    .requestApi(kafkaProtocolParsedMessage.getRequestApi())
                    .requestTopic(topicName)
                    .executeTime(System.currentTimeMillis())
                    .responseDataLength(kafkaProtocolParsedMessage.getResponseLength())
                    .responseRecord(resTopic)
                    .build();
            data.add(protocolParseData);
        }
        return data;
    }
}
