package cn.rtm.protocol.parser;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.BiFunction;


/**
 *  该类主要负责执行协议解析模板方法，提供了协议常规解析逻辑，协议结构约束为： ProtocolMessage =》 Size Header Payload
 *
 * <ul>
 * <li> 方法 {@link #init(ProtocolMessage)} 进行初始化解析内容
 * <li> 方法 {@link #parse(ProtocolMessage)} 为统一解析入口，对外提供解析逻辑
 * <li> 方法 {@link #doParse(ByteBuffer, BiFunction, BiFunction)} 协议解析模板方法，实现依次解析请求头、请求体、构建解析结果
 * <li> 不同协议解析子类，分别实现抽象方法 {@link #parseHeader(ByteBuffer)} 、{@link #parseBody(Object, ByteBuffer)}
 * 执行具体的解析过程、通过 {@link #buildParsedMessage(Object, Object)} 组装解析结果
 * </ul>
 * @param <H> 解析的协议请求头类型
 * @param <B> 解析的协议请求体类型
 * @param <R> 解析的协议结果类型
 */
public abstract class AbstractProtocolParser<H,B,R> implements ProtocolParser<ProtocolMessage, R> {


    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     *  捕获的协议原始数据内容，包含公共通信信息
     */
    protected ThreadLocal<ProtocolMessage> data = new ThreadLocal<>();

    /**
     *  协议上下文，负责实现请求-响应数据包数据传递
     */
    protected final ProtocolContext protocolContext;

    /**
     *  请求数据包解析包开始时间
     */
    protected LocalDateTime startParseTime;

    public AbstractProtocolParser(ProtocolContext protocolContext) {
        this.protocolContext = protocolContext;
    }

    @Override
    public R parse(ProtocolMessage packet) {

        this.init(packet);

        return this.doParse(this.getTcpPayloadWithNoLength(packet),this::parseBody,this::buildParsedMessage);
    }


    /**
     * 请求数据包解析初始化
     * @param packet 请求数据包
     */
    private void init(ProtocolMessage packet) {
        this.data.set(packet);
        this.startParseTime = LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
        this.beforeParse();
    }


    /**
     *  获取 tcp 传输的消息体，不包含消息体长度本身（kafka 前 4个字节），其他协议可自定义实现
     * @return 返回需要解析的协议内容
     */
    protected ByteBuffer getTcpPayloadWithNoLength(ProtocolMessage packet) {
        return packet.rawDataWithNoLength();
    }


    /**
     * 执行真正的协议解析
     * @param buffer 待解析的协议数据包
     * @param bodyParser 请求体解析器
     * @param messageBuilder 解析结果构造器
     * @return 返回解析后的协议内容
     */
    protected R doParse(ByteBuffer buffer, BiFunction<H,ByteBuffer,B> bodyParser, BiFunction<H,B,R> messageBuilder) {
        H header = parseHeader(buffer);
        B body = bodyParser.apply(header, buffer);
        return messageBuilder.apply(header, body);
    }

    /**
     *  解析前开始执行，子类可根据需要执行其逻辑
     */
    protected void beforeParse() {
    }


    /**
     *  解析协议头信息
     * @param buffer 待解析的协议数据包
     * @return 返回解析后的协议头内容
     */
    protected abstract H parseHeader(ByteBuffer buffer);


    /**
     *  解析协议 body 信息
     * @param header 解析完成的请求头内容
     * @param buffer 待解析的协议数据包
     * @return 返回解析后的协议 body 内容
     */
    protected abstract B parseBody(H header, ByteBuffer buffer);


    /**
     *  组装完整解析的协议内容
     * @param header 解析的协议 header 信息
     * @param body 解析完成的协议 body 信息
     * @return 返回完整数据包解析内容
     */
    protected abstract R buildParsedMessage(H header, B body);

    /**
     *  获取协议上下文对象，包含整个请求-响应解析过程中的参数信息
     * @return 返回协议上下文对象
     */
    public ProtocolContext getProtocolContext() {
        return protocolContext;
    }

    /**
     *  判断是否为目标请求数据包
     * @return 返回 true 则是请求数据包， 反之 false
     */
    public boolean isRequestPacket() {
        return getCommonData().isRequestPacket();
    }

    /**
     *  判断是否为目标响应数据包
     * @return 返回 true 则是请求数据包， 反之 false
     */
    public boolean isResponsePacket() {
        return getCommonData().isResponsePacket();
    }

    /**
     *  获取协议公共解析信息
     * @return 返回请求数据包公共解析信息
     */
    public ProtocolMessage getCommonData() {
        return data.get();
    }
}
