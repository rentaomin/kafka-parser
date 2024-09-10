package cn.rtm.kafkaParser;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
public class KafkaParserApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaParserApplication.class, args);
    }

}
