package cn.rtm.kafkaParser.protocol.enums;

/**
 *  支持解析的协议类型
 */
public enum ProtocolType {

    KAFKA("KafKA" , " Kafka 协议类型");

    private String name;

    private String description;

    ProtocolType(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
