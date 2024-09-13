package cn.rtm.protocol.parser;

import org.pcap4j.packet.Packet;

/**
 *  数据包组合器，若存在分片传输返回合并后完整的数据包
 * @param <P> 合并后的数据包
 */
@FunctionalInterface
public interface PacketReassemble<P> {


    /**
     *  合并数据包，若存在分片传输，则返回完整的数据包，等同于 wireshark reassembled
     * @param packet 网卡捕获的数据包
     * @return 返回完整的数据包内容, 若数据包未合并完成或者未携带有效数据则返回 null
     */
   P reassemble(Packet packet);

}
