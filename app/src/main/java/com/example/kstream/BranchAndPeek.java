package com.example.kstream;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import com.example.kstream.util.SerdesFactory;
import com.example.kstream.util.StreamUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BranchAndPeek {

    static Logger logger = LoggerFactory.getLogger(JsonToAvro.class);
    public static void main(String[] argc) {
        Properties config;
        try {
            config = StreamUtil.getInitialProps("branch-and-peek");
            String[] sinkTopics  = {"low_quantity_purchase","high_quantity_purchase"};
            KafkaStreams stream = BranchAndPeek.buildJsonToAvroStream(config, "syslog_avro", sinkTopics);
            logger.info(stream.toString());
            stream.start();
    
            Runtime.getRuntime().addShutdownHook(new Thread(stream::close));    
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static KafkaStreams buildJsonToAvroStream(final Properties config, final String sourceTopic,final String[] sinkTopic ) {
        SerdesFactory.setSerdesConfig(config);

        final StreamsBuilder builder = new StreamsBuilder();

        // add code here
        
        return new KafkaStreams(builder.build(), config);
    }
}
