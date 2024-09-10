package cn.rtm.kafkaParser.protocol.extractor;

import cn.rtm.kafkaParser.protocol.DataParseExtractor;

/**
 *  请求数据解析结果提取器,若需要灵活实现请求数据解析，实现该接口
 */
public interface RequestDataParseExtractor<M,T> extends DataParseExtractor<M,T> {
}
