package cn.rtm.kafkaParser.protocol.parser;

import cn.rtm.kafkaParser.protocol.ProtocolContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  kafka 协议解析默认实现，提供基础的参数传递
 */
public class DefaultProtocolContext implements ProtocolContext {

    private Logger log = LoggerFactory.getLogger(getClass());

    /**
     *  存储上下文传递的参数信息
     */
    private final Map<Object, Object> container = new ConcurrentHashMap<>();

    @Override
    public void addParam(Object key, Object value) {
        container.put(key,value);
    }

    @Override
    public Object getParam(Object key) {
        return container.get(key);
    }

    @Override
    public <T> T getParamAs(Object key, Class<T> type) {
        Object value = getParam(key);
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        return null;
    }
}
