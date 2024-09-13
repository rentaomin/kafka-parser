package cn.rtm.protocol.parser;

/**
 *  数据解析提取消费者
 * @param <D> 提取的解析数据内容
 */
@FunctionalInterface
public interface DataParseExtractConsumer<D>{

    /**
     *  根据给定的数据执行该操作
     * @param extractData 提取的解析数据内容
     */
    void accept(D extractData);
}
