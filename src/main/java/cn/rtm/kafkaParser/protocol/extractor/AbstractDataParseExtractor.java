package cn.rtm.kafkaParser.protocol.extractor;

import cn.rtm.kafkaParser.protocol.KafkaData;
import cn.rtm.kafkaParser.protocol.KafkaProtocolParsedMessage;
import cn.rtm.kafkaParser.protocol.ProtocolMessage;
import cn.rtm.kafkaParser.protocol.DataParseExtractor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;


/**
 * 提供数据公共解析模板方法,子类分别实现请求数据解析方法, 若扩展子类实现，需要通过{@link DataParseExtractSupplier#register()} 进行实例化
 *  <ul>
 *  <li> 实现请求数据解析方法 {@link #extractRequest(ApiMessage)}
 *  <li> 实现响应数据解析方法 {@link #extractResponse(ApiMessage)}
 *  <li> 实现解析结果数据构建方法 {@link #composeData(KafkaProtocolParsedMessage, Object, Object)}
 *  </ul>
 * @param <ReqType> 请求数据解析结果类型
 * @param <ResType> 响应数据解析结果类型
 * @param <ReqData> 提取的请求数据结果类型
 * @param <ResData> 提取的响应数据结果类型
 */
public abstract class AbstractDataParseExtractor<ReqType extends ApiMessage, ResType extends ApiMessage,
        ReqData, ResData> implements DataParseExtractor<KafkaProtocolParsedMessage,List<KafkaData>> {

    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     *  请求数据解析结果类型，对应 kafka 源码包：org.apache.kafka.common.kafkaProtocolParsedMessage 下，
     *  以 *RequestData 结尾请求实体数据类型，子类根据对应提取的数据指定对应类型
     */
    private final Class<ReqType> requestClass;

    /**
     *  请求数据解析结果类型，对应 kafka 源码包：org.apache.kafka.common.kafkaProtocolParsedMessage 下，
     *   以 *ResponseData 结尾响应实体数据类型，子类根据对应提取的数据指定对应类型
     */
    private final Class<ResType> responseClass;

    public AbstractDataParseExtractor(Class<ReqType> requestClass, Class<ResType> responseClass) {
        this.requestClass = requestClass;
        this.responseClass = responseClass;
    }

    /**
     *  协议解析后的内容
     */
    private KafkaProtocolParsedMessage kafkaProtocolParsedMessage;

    /**
     *  kafka record key 反序列器
     */
    private Deserializer<String> keyDeserializer = new StringDeserializer();

    /**
     *  kafka record value 反序列器
     */
    private Deserializer<String> valueDeserializer = new StringDeserializer();

    /**
     *  最大提取的数据量
     */
    private final static int maxExtractDataSize = 10;

    @Override
    public List<KafkaData> extract(KafkaProtocolParsedMessage kafkaProtocolParsedMessage) {
        this.kafkaProtocolParsedMessage = kafkaProtocolParsedMessage;
        if (kafkaProtocolParsedMessage == null || !kafkaProtocolParsedMessage.isParseComplete()){
            return null;
        }
        ApiMessage requestMessage = kafkaProtocolParsedMessage.getRequestMessage();
        ApiMessage responseMessage = kafkaProtocolParsedMessage.getResponseMessage();

        ReqType requestData = null;
        if (requestClass.isInstance(requestMessage)) {
            requestData = requestClass.cast(requestMessage);
        }

        ResType responseRecord = null;
        if (responseClass.isInstance(responseMessage)) {
            responseRecord = responseClass.cast(responseMessage);
        }
        return composeData(kafkaProtocolParsedMessage, extractRequest(requestData), extractResponse(responseRecord));
    }


    /**
     *  提取请求协议包解析结果数据内容
     * @param requestMessage 解析后的的请求数据包内容
     * @return 返回提取的请求数据
     */
    protected abstract ReqData extractRequest(ReqType requestMessage);

    /**
     * 提取响应协议包解析结果数据内容
     * @param responseMessage 解析后的的响应数据包内容
     * @return 返回提取的响应数据
     */
    protected abstract ResData extractResponse(ResType responseMessage);

    /**
     * 负责组合提取的数据内容
     * @param kafkaProtocolParsedMessage 协议解析结果
     * @param requestData 提取的请求数据包内容
     * @param responseRecord 提取的响应数据包内容
     * @return 返回包含请求和响应提取的数据包完整内容
     */
    protected abstract List<KafkaData> composeData(KafkaProtocolParsedMessage kafkaProtocolParsedMessage, ReqData requestData, ResData responseRecord);


    /**
     *  解析 kafka record 内容
     * @param topic 解析的 topic 名称
     * @param memoryRecords 待提取数据的 record 记录
     * @param maxPollSize 最大提取记录数量
     * @return 返回提取后的数据内容
     */
    public List<String> extractRecord(String topic, MemoryRecords memoryRecords, int maxPollSize) {
        List<String> recordValues = new ArrayList<>(maxPollSize);
        int readSize = 0;
        AbstractIterator<MutableRecordBatch> it = memoryRecords.batchIterator();
        while (it.hasNext()) {
            MutableRecordBatch next = it.next();
            if (next == null) {
                break;
            }
            Iterator<Record> recordIterator = next.iterator();
            if (readSize >= maxPollSize) {
                break;
            }
            while (recordIterator.hasNext()) {
                Record record = recordIterator.next();
                if (record == null) {
                    break;
                }
                try {
                    if (readSize >= maxPollSize) {
                        break;
                    }
                    Headers headers = new RecordHeaders(record.headers());

                    ByteBuffer keyBytes = record.key();
                    byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
                    String keyValue = keyBytes == null ? null : getKeyDeserializer().deserialize(topic, headers, keyByteArray);

                    ByteBuffer valueBytes = record.value();
                    byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
                    String recordValue = valueBytes == null ? null : getValueDeserializer().deserialize(topic, headers, valueByteArray);

                    recordValues.add(recordValue);
                    readSize++;
                } catch (Exception e) {
                    log.error("解析 Kafka record key or value 出错！", e);
                }
            }
        }
        System.out.println("共计："+readSize);
        return recordValues;
    }

    /**
     * 实现消息数量均匀抽数，获取每个 topic 提取的数据量
     * @param topics 需要获取内容的topic 信息
     * @param expectTotalPollSize 总的拉取数量
     * @param keyGenerator key 生成器
     * @return 返回拉取结果，key: 获取数据的 topic 名称， Value: 该 topic 需要拉取的数量
     * @param <T> 需要提取数据的 topic 内容
     */
    protected static <T> Map<String,Integer> expectExtractSizeFor(Collection<T> topics, int expectTotalPollSize, Function<T,String> keyGenerator) {
        int topicCount = topics.size();
        Map<String,Integer> topicPollCount = new HashMap<>(topicCount);
        int baseCount = expectTotalPollSize / topicCount;
        int retainCount = expectTotalPollSize % topicCount;
        int i = 0;
        for (T topic: topics) {
            if (i < retainCount) {
                baseCount++;
            }
            topicPollCount.put(keyGenerator.apply(topic), baseCount);
            i++;
        }
        return topicPollCount;
    }


    /**
     *  获取数据包源请求基础信息
     * @return 返回协议包请求-响应基础信息
     */
    protected ProtocolMessage getOriginData() {
        if (this.kafkaProtocolParsedMessage == null) {
            return null;
        }
        return kafkaProtocolParsedMessage.getOriginData();
    }


    /**
     *  自定义 kafka kafkaProtocolParsedMessage key 反序列化解析器，用于提取 kafka value 内容
     * @param keyDeserializer kafka kafkaProtocolParsedMessage key 反序列化解析器
     */
    protected void setKeyDeserializer(Deserializer<String> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }


    /**
     *  自定义 kafka kafkaProtocolParsedMessage value 反序列化解析器，用于提取 kafka value 内容
     * @param valueDeserializer kafka kafkaProtocolParsedMessage value 反序列化解析器
     */
    protected void setValueDeserializer(Deserializer<String> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }


    /**
     *  获取 Kafka record key 反序列器,默认为：{@link StringDeserializer}
     * @return 返回 kafka kafkaProtocolParsedMessage key 反序列化解析器
     */
    protected Deserializer<String> getKeyDeserializer() {
        return keyDeserializer;
    }


    /**
     *  获取 Kafka record value 反序列器,默认为：{@link StringDeserializer}
     * @return 返回 反序列器
     */
    protected Deserializer<String> getValueDeserializer() {
        return valueDeserializer;
    }


    /**
     *  获取最大提取数据的数量
     * @return 返回提取数量
     */
    protected int getMaxExtractDataSize() {
        return maxExtractDataSize;
    }


    /**
     *  判断是否为 kafka 默认元数据 topic
     * @param topic 解析的 topic 名称
     * @return 返回 true 则是 kafka 内置 topic,返回 false 则反之
     */
    public boolean isKafkaMetaDataTopic(String topic) {
        return Topic.GROUP_METADATA_TOPIC_NAME.equals(topic);
    }


    /**
     *  是否不是 kafka 内置元数据 topic 信息
     * @param topic 解析的 topic 名称
     * @return 返回 true 则不是内置的 topic ，反之 false
     */
    public boolean isNotKafkaMetaDataTopic(String topic) {
        return !isKafkaMetaDataTopic(topic);
    }
}
