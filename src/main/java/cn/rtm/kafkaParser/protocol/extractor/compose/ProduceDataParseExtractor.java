package cn.rtm.kafkaParser.protocol.extractor.compose;

import cn.rtm.kafkaParser.protocol.handler.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.ProtocolParseData;
import cn.rtm.kafkaParser.protocol.extractor.AbstractDataParseExtractor;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 *  负责提取 kafka-Produce Api 请求解析内容 {@linkplain  org.apache.kafka.common.message.ProduceRequestData}和
 *   响应解析内容 {@linkplain  org.apache.kafka.common.message.ProduceResponseData}
 */
public class ProduceDataParseExtractor extends AbstractDataParseExtractor<ProduceRequestData,ProduceResponseData,Map<String,List<String>>,List<String>> {

    public ProduceDataParseExtractor() {
        super(ProduceRequestData.class, ProduceResponseData.class);
    }


    @Override
    protected Map<String, List<String>> extractRequest(ProduceRequestData produceRequestData) {
        int maxPollSize = getMaxExtractDataSize();
        ProduceRequestData.TopicProduceDataCollection topicProduceData = produceRequestData.topicData();
        Map<String, Integer> topicPollSize = expectExtractSizeFor(topicProduceData, maxPollSize, this::getTopic);
        Map<String,List<String>> extractData = new HashMap<>(maxPollSize);
        for (ProduceRequestData.TopicProduceData topic : topicProduceData) {
            String topicName = topic.name();
            List<ProduceRequestData.PartitionProduceData> partitionProduceData = topic.partitionData();
            for (ProduceRequestData.PartitionProduceData partition: partitionProduceData) {
                BaseRecords records = partition.records();
                if (records instanceof MemoryRecords) {
                    MemoryRecords memoryRecords = (MemoryRecords) records;
                    int pollSize = topicPollSize.get(topicName);
                    List<String> recordValues = extractRecord(topicName, memoryRecords, pollSize);
                    System.out.println(recordValues.size());
                    extractData.put(topicName, recordValues);
                }
            }
        }
        return extractData;
    }


    @Override
    protected List<String> extractResponse(ProduceResponseData produceResponseData) {
        ProduceResponseData.TopicProduceResponseCollection responses = produceResponseData.responses();
        if (responses == null) {
            return null;
        }
        return responses.stream().map(ProduceResponseData.TopicProduceResponse::name)
                .collect(Collectors.toList());
    }


    @Override
    protected List<ProtocolParseData> composeData(KafkaProtocolParsedMessage kafkaProtocolParsedMessage, Map<String, List<String>> requestData, List<String> responseRecord) {
        if (MapUtils.isEmpty(requestData)) {
            return Collections.emptyList();
        }
        ProtocolMessage originData = getOriginData();
        return requestData.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(recordValue -> new ProtocolParseData.Builder()
                                .srcIp(originData.getSrcIp())
                                .srcPort(originData.getSrcPort())
                                .destIp(originData.getDestIp())
                                .destPort(originData.getDestPort())
                                .clientId(kafkaProtocolParsedMessage.getRequestHeader().clientId())
                                .requestApi(kafkaProtocolParsedMessage.getRequestApi())
                                .requestTopic(entry.getKey())  // 使用 entry 的 key 作为 topicName
                                .executeTime(System.currentTimeMillis())
                                .responseDataLength(kafkaProtocolParsedMessage.getResponseLength())
                                .responseRecord(recordValue)
                                .build())
                ).collect(Collectors.toList());
    }


    /**
     *  获取 topic 名称
     * @param response 响应数据内容
     * @return 返回 topic 名称
     */
    private String getTopic(ProduceRequestData.TopicProduceData response) {
        if (response == null) {
            return "";
        }
        return response.name();
    }

}
