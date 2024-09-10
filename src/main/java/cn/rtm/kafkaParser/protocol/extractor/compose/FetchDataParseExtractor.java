package cn.rtm.kafkaParser.protocol.extractor.compose;

import cn.rtm.kafkaParser.protocol.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.extractor.AbstractDataParseExtractor;
import cn.rtm.kafkaParser.protocol.KafkaData;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import java.util.*;
import java.util.stream.Collectors;

/**
 *  负责提取 kafka-Fetch Api 请求解析内容 {@linkplain  org.apache.kafka.common.message.FetchRequestData }和
 *  响应解析内容 {@linkplain  org.apache.kafka.common.message.FetchResponseData}
 */
public class FetchDataParseExtractor extends AbstractDataParseExtractor<FetchRequestData,FetchResponseData,List<String>,Map<String,List<String>>> {

    public FetchDataParseExtractor() {
        super(FetchRequestData.class, FetchResponseData.class);
    }

    @Override
    protected List<String> extractRequest(FetchRequestData data) {
        return Optional.ofNullable(data.topics())
                .orElse(new ArrayList<>())
                .stream()
                .map(FetchRequestData.FetchTopic ::topic)
                .filter(this::isNotKafkaMetaDataTopic)
                .collect(Collectors.toList());
    }


    @Override
    protected Map<String, List<String>> extractResponse(FetchResponseData fetchResponseData) {
        List<FetchResponseData.FetchableTopicResponse> responses = fetchResponseData.responses();
        if (CollectionUtils.isEmpty(responses)) {
            return null;
        }

        int maxPollSize = getMaxExtractDataSize();
        Map<String, Integer> topicPollSize = expectExtractSizeFor(fetchResponseData.responses(), maxPollSize, this::getTopic);

        Map<String,List<String>> extractData = new HashMap<>(maxPollSize);
        for (FetchResponseData.FetchableTopicResponse response : fetchResponseData.responses()) {
            String topic = response.topic();
            if (isKafkaMetaDataTopic(topic)) {
                continue;
            }
            List<FetchResponseData.PartitionData> partitions = response.partitions();
            for (FetchResponseData.PartitionData partition : partitions) {
                BaseRecords records = partition.records();
                if (records instanceof MemoryRecords) {
                    MemoryRecords memoryRecords = (MemoryRecords) records;
                    int pollSize = topicPollSize.get(topic);
                    List<String> recordValues = extractRecord(topic,memoryRecords, pollSize);
                    extractData.put(topic, recordValues);
                }
            }
        }
        return extractData;
    }


    @Override
    protected List<KafkaData> composeData(KafkaProtocolParsedMessage kafkaProtocolParsedMessage, List<String> requestData, Map<String, List<String>> responseRecord) {
        if (CollectionUtils.isEmpty(requestData)) {
            return Collections.emptyList();
        }
        List<KafkaData> data = new ArrayList<>();
        for (String topicName : requestData) {
            List<String> topicValues = responseRecord.get(topicName);
            for (String record : topicValues) {
                KafkaData kafkaData = buildKafkaData(kafkaProtocolParsedMessage,topicName,record);
                data.add(kafkaData);
            }
        }
        return data;
    }


    /**
     *  构建 kafka 提取的数据内容
     * @param kafkaProtocolParsedMessage 解析的数据包内容
     * @param request 提取的请求数据
     * @param response 提取的响应数据
     * @return 返回组合后的完整数据内容
     */
    private KafkaData buildKafkaData(KafkaProtocolParsedMessage kafkaProtocolParsedMessage, String request, String response) {
        ProtocolMessage originData = getOriginData();
        return new KafkaData.Builder()
                .srcIp(originData.getSrcIp())
                .srcPort(originData.getSrcPort())
                .destIp(originData.getDestIp())
                .destPort(originData.getDestPort())
                .clientId(kafkaProtocolParsedMessage.getRequestHeader().clientId())
                .requestApi(kafkaProtocolParsedMessage.getRequestApi())
                .requestTopic(request)
                .executeTime(System.currentTimeMillis())
                .responseDataLength(kafkaProtocolParsedMessage.getResponseLength())
                .responseRecord(response)
                .build();
    }


    /**
     *  获取提取数据的 topic 名称
     * @param response 响应结果内容
     * @return 返回当前解析结果 topic 名称
     */
    private String getTopic(FetchResponseData.FetchableTopicResponse response) {
        if (response == null) {
            return "";
        }
        return response.topic();
    }

}
