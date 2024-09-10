package cn.rtm.kafkaParser.protocol.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class JsonUtil {

    public static Map<String, String> flattenedMap(String recordValue) {
        Map<String, String> flattenedMap = new HashMap<>();
        if (isValidJson(recordValue)) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = null;
            try {
                rootNode = objectMapper.readTree(recordValue);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            flatten("", rootNode, flattenedMap);
        }
        return flattenedMap;
    }


    /**
     *  判断当前字符串是否为有效的 json 格式
     * @param json 字符串内容
     * @return 返回 true 则是 json 格式 ，反之 false
     */
    public static boolean isValidJson(String json) {
        if (StringUtils.isBlank(json)) {
            return false;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(json);
            return rootNode != null;
        } catch (Exception e) {
            // 如果抛出异常，说明不是合法的 JSON
            return false;
        }
    }


    /**
     *  扁平化 json 字符串，转换为 Map 结构
     * @param parentKey 负极根节点标识
     * @param node 带解析的 json 节点信息
     * @param flattenedMap 存储解析的内容,key: json 节点key ,多层级通过 ”。“分割 ；value: 节点 key 对应的值
     */
    public static void flatten(String parentKey, JsonNode node, Map<String, String> flattenedMap) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = parentKey.isEmpty() ? field.getKey() : parentKey + "." + field.getKey();
                flatten(fieldName, field.getValue(), flattenedMap);
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String arrayKey = parentKey + "[" + i + "]";
                flatten(arrayKey, node.get(i), flattenedMap);
            }
        } else {
            flattenedMap.put(parentKey, node.asText());
        }
    }
}
