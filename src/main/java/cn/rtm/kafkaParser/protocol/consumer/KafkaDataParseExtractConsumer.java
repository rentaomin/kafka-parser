package cn.rtm.kafkaParser.protocol.consumer;

import cn.rtm.kafkaParser.protocol.DataParseExtractConsumer;
import cn.rtm.kafkaParser.protocol.KafkaData;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 *  该类的主要职责是将解析提取的数据内容执行 {@link #accept(List)} 操作，与数据提取解耦
 */
public class KafkaDataParseExtractConsumer implements DataParseExtractConsumer<List<KafkaData>> {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void accept(List<KafkaData> extractData) {
        log.error("start cunsume data!");
        if (CollectionUtils.isEmpty(extractData)) {
            return;
        }

    }
}
