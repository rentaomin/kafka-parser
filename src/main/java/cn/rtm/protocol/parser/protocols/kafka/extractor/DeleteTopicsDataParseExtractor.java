package cn.rtm.protocol.parser.protocols.kafka.extractor;

import cn.rtm.protocol.parser.protocols.kafka.KafkaProtocolParsedMessage;
import cn.rtm.protocol.parser.ProtocolMessage;
import cn.rtm.protocol.parser.protocols.kafka.AbstractDataParseExtractor;
import cn.rtm.protocol.parser.ProtocolParseData;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *  负责提取 Kafka-DeleteTopics 请求解析内容 {@linkplain  org.apache.kafka.common.message.DeleteTopicsRequestData }和
 *  响应解析内容 {@linkplain  org.apache.kafka.common.message.DeleteTopicsResponseData}
 */
public class DeleteTopicsDataParseExtractor extends AbstractDataParseExtractor<DeleteTopicsRequestData,DeleteTopicsResponseData,List<String> ,List<String>> {

    public DeleteTopicsDataParseExtractor() {
        super(DeleteTopicsRequestData.class, DeleteTopicsResponseData.class);
    }

    @Override
    protected List<String> extractRequest(DeleteTopicsRequestData requestData) {
        return requestData.topicNames();
    }

    @Override
    protected List<String> extractResponse(DeleteTopicsResponseData responseData) {
        DeleteTopicsResponseData.DeletableTopicResultCollection responses = responseData.responses();
        if (responses == null) {
            return null;
        }
        return responses.stream()
                .map(DeleteTopicsResponseData.DeletableTopicResult::name)
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
                    .requestData(topicName)
                    .startTime(kafkaProtocolParsedMessage.getStartTime())
                    .endTime(kafkaProtocolParsedMessage.getEndTime())
                    .executeTime(System.currentTimeMillis())
                    .responseDataLength(kafkaProtocolParsedMessage.getResponseLength())
                    .responseData(resTopic)
                    .build();
            data.add(protocolParseData);
        }
        return data;
    }

}
