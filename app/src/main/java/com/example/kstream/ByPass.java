package com.example.kstream;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.example.kstream.util.SerdesFactory;
import com.example.kstream.util.StreamUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByPass {

    static Logger logger = LoggerFactory.getLogger(JsonToAvro.class);

    public static void main(String[] argc) {
        Properties config;
        try {
            config = StreamUtil.getInitialProps("bypass");
            KafkaStreams stream = ByPass.buildJsonToAvroStream(config, "input-topic", "output-topic");
            Runtime.getRuntime().addShutdownHook(new Thread(stream::close));    
            stream.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    static KafkaStreams buildJsonToAvroStream(final Properties config, final String sourceTopic,final String sinkTopic ) {
        SerdesFactory.setSerdesConfig(config);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));

        stream.to(sinkTopic, Produced.with(Serdes.String(),Serdes.String()));
        
        return new KafkaStreams(builder.build(), config);
    }
}
