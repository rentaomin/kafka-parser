package cn.rtm.protocol.parser.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemo {



    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducerDemo kafkaProducerDemo =  new KafkaProducerDemo();
        kafkaProducerDemo.send();
    }

    public void send() throws ExecutionException, InterruptedException {
        Producer<String,String> producer = new KafkaProducer<>(producerConfigs());
//        String topic = "configData";
        String topic = "del-topic2";
        String value = "{\"systemId\":9,\"dataId\":\"default_rule_63\",\"dataType\":0,\"operateType\":1,\"creater\":\"B5A33586-15104954-9B752AE1-53333EC10\",\"messageId\":\"4e7a1ab529654ff2af636c100c7330f7\",\"dataName\":\"VB源代码\",\"account\":\"admin\"}";
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,value);
        RecordMetadata recordMetadata = producer.send(record).get();
        producer.close();
    }

    public static Map<String, Object> producerConfigs() {
        System.out.println("-----------------读取kafka配置------------------");
        Map<String, Object> props = new HashMap<>();
        // 指定多个kafka集群多个地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.71.233:9094");
        // 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}
