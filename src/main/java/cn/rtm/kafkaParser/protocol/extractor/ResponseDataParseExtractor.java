package cn.rtm.kafkaParser.protocol.extractor;

import cn.rtm.kafkaParser.protocol.DataParseExtractor;

/**
 *  响应数据解析结果提取器,若需要灵活实现响应数据解析，实现该接口
 */
public interface ResponseDataParseExtractor<M,T> extends DataParseExtractor<M,T> {
}
