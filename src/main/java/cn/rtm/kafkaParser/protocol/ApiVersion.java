package cn.rtm.kafkaParser.protocol;

/**
 *  kafka api 版本号
 */
public enum ApiVersion {

    V0((short) 0, "Version: 0"),
    V1((short) 1, "Version: 1"),
    V2((short) 2, "Version: 2"),
    V3((short) 3, "Version: 3"),
    V4((short) 4, "Version: 4"),
    V5((short) 5, "Version: 5"),
    V6((short) 6, "Version: 6"),
    V7((short) 7, "Version: 7"),
    V8((short) 8, "Version: 8"),
    V9((short) 9, "Version: 9"),
    V10((short) 10, "Version: 10"),
    V11((short) 11, "Version: 11"),
    V12((short) 12, "Version: 12"),
    V13((short) 13, "Version: 13"),
    V14((short) 14, "Version: 14"),
    V15((short) 15, "Version: 15"),
    V16((short) 16, "Version: 16"),
    ;

    private short version;

    private String description;

    ApiVersion(short version, String description) {
        this.version = version;
        this.description = description;
    }
    public short getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }
}
