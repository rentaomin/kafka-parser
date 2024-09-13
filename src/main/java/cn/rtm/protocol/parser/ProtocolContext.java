package cn.rtm.protocol.parser;

/**
 *  该接口主要提供在数据包解析过程中需要的上下文参数信息
 */
public interface ProtocolContext {

    /**
     *  添加参数信息
     * @param key 参数标识
     * @param value 参数值
     */
    void addParam(Object key, Object value);


    /**
     *  根据指定 key 查找对应的参数值
     * @param key 参数标识
     * @return 返回参数值，不存在则返回 null
     */
    Object getParam(Object key);


    /**
     *  查找指定 类型和指定 key 的参数值
     * @param key 参数标识
     * @param type 参数值的类型
     * @return 返回参数值，不存在则返回 null
     * @param <T> 对应的参数值类型
     */
    <T> T getParamAs(Object key, Class<T> type);

}
