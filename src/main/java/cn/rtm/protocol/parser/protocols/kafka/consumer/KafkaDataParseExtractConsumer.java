package cn.rtm.protocol.parser.protocols.kafka.consumer;

import cn.rtm.protocol.parser.DataParseExtractConsumer;
import cn.rtm.protocol.parser.ProtocolParseData;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 *  该类的主要职责是将解析提取的数据内容执行 {@link #accept(List)} 操作，与数据提取解耦
 */
public class KafkaDataParseExtractConsumer implements DataParseExtractConsumer<List<ProtocolParseData>> {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void accept(List<ProtocolParseData> extractData) {
        if (CollectionUtils.isEmpty(extractData)) {
            return;
        }
        System.out.println(extractData.size());

    }
}
