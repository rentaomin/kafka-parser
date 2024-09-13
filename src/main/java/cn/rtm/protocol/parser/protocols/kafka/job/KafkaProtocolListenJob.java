package cn.rtm.protocol.parser.protocols.kafka.job;


import cn.rtm.protocol.parser.ProtocolParseHandler;
import org.pcap4j.core.BpfProgram;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 *  捕获 kafka 协议数据包
 */
@Component
public class KafkaProtocolListenJob implements ApplicationRunner {

    static short LISTEN_PORT = 9094;

    @Resource
    private ProtocolParseHandler kafkaProtocolParseHandler;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        // 获取所有网络接口
        PcapNetworkInterface nif = Pcaps.findAllDevs()
                .stream()
                // 选择一个网络接口
                .filter(dev -> dev.getAddresses().stream().filter(inet -> inet.getAddress().getHostAddress().equals("192.168.146.138"))
                        .findAny().isPresent())
                .findAny()
                .get();

        // 创建用于捕获数据包的PcapHandle
//        PcapHandle handle = new PcapHandle.Builder(nif.getName())
//                .snaplen(65536)    // 捕获的数据包的最大字节数
//                .promiscuousMode(PcapNetworkInterface.PromiscuousMode.PROMISCUOUS)  // 混杂模式
//                .timeoutMillis(10)  // 超时时间
//                .build();

        // 打开网络接口
        int snapLen = 65536;           // 捕获的最大数据包大小
        int timeout = 10 * 1000;       // 超时时间（毫秒）
        PcapHandle handle = nif.openLive(snapLen, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, timeout);
        // 设置过滤器，只捕获发往MySQL默认端口（3306）的TCP流量
        handle.setFilter("tcp port "+String.valueOf(LISTEN_PORT), BpfProgram.BpfCompileMode.OPTIMIZE);
        // 捕获数据包
        try {
            while (true) {
                Packet packet = null;
                try {
                    packet = handle.getNextPacketEx();
                } catch (Exception e) {
                    System.out.println("异常");
                }
                if (packet != null) {
                    parsePacket(packet);
                }
            }
        } catch (Exception e) {
            System.out.println("捕获结束.");
        } finally {
            handle.close();
        }
    }

    // 判断是否是我们感兴趣的端口
    private boolean isPortOfInterest(int srcPort, int dstPort) {
        List<Integer> interestedPorts = Arrays.asList(9094, 9092,5353); // 替换成你想监听的端口
        return interestedPorts.contains(srcPort) || interestedPorts.contains(dstPort);
    }

    private void parsePacket(Packet packet) {
        kafkaProtocolParseHandler.handle(packet);
    }


}
