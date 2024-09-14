package cn.rtm.protocol.parser.protocols.redis.parser.response;

import cn.rtm.protocol.parser.AbstractProtocolParser;
import cn.rtm.protocol.parser.ProtocolContext;
import cn.rtm.protocol.parser.protocols.redis.RedisBody;
import cn.rtm.protocol.parser.protocols.redis.RedisHeader;
import cn.rtm.protocol.parser.protocols.redis.RedisParseData;
import java.nio.ByteBuffer;

public class RedisResponseParser extends AbstractProtocolParser<RedisHeader, RedisBody, RedisParseData> {

    public RedisResponseParser(ProtocolContext protocolContext) {
        super(protocolContext);
    }

    @Override
    protected RedisHeader parseHeader(ByteBuffer buffer) {
        return null;
    }

    @Override
    protected RedisBody parseBody(RedisHeader header, ByteBuffer buffer) {
        return null;
    }

    @Override
    protected RedisParseData buildParsedMessage(RedisHeader header, RedisBody body) {
        return null;
    }
}
