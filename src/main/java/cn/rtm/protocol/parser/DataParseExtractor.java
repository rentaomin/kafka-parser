package cn.rtm.protocol.parser;

/**
 *  主要负责从解析后的数据提取需要的数据内容
 * @param <M> 解析后的源数据信息
 * @param <T> 提取的数据信息
 */
@FunctionalInterface
public interface DataParseExtractor<M,T> {


    /**
     *  从解析后的协议消息中提取所需要的内容
     * @param message 解析后的协议内容
     * @return 返回需要提取的目标内容
     */
    T extract(M message);
}
