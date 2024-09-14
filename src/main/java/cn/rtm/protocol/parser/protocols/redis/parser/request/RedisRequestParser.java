package cn.rtm.protocol.parser.protocols.redis.parser.request;

import cn.rtm.protocol.parser.AbstractProtocolParser;
import cn.rtm.protocol.parser.ProtocolContext;
import cn.rtm.protocol.parser.protocols.redis.RedisBody;
import cn.rtm.protocol.parser.protocols.redis.RedisHeader;
import cn.rtm.protocol.parser.protocols.redis.RedisParseData;
import java.nio.ByteBuffer;

public class RedisRequestParser extends AbstractProtocolParser<RedisHeader, RedisBody, RedisParseData> {

    public RedisRequestParser(ProtocolContext protocolContext) {
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
