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

public class Branch {

    static Logger logger = LoggerFactory.getLogger(JsonToAvro.class);

    public static void main(String[] argc) {
        Properties config;
        try {
            config = StreamUtil.getInitialProps("branch-with-length");
            String[] sinkTopics  = {"output-topic","error-topic"};
            KafkaStreams stream = Branch.buildJsonToAvroStream(config, "input-topic", sinkTopics);
            Runtime.getRuntime().addShutdownHook(new Thread(stream::close));    
            stream.start();
    
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    static KafkaStreams buildJsonToAvroStream(final Properties config, final String sourceTopic,final String[] sinkTopic ) {
        SerdesFactory.setSerdesConfig(config);

        final StreamsBuilder builder = new StreamsBuilder();

        // read the source stream
        final KStream<String, String> branchStream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));

        Map<String, KStream<String,String>> branchedStreams = branchStream.split(Named.as("length-"))
        .branch((id, str) -> (str != null) && (str.length() < 5), Branched.as("success"))
        .defaultBranch(Branched.as("error"));
        
        branchedStreams.get("length-success").to(sinkTopic[0], Produced.with(Serdes.String(),Serdes.String()));
        branchedStreams.get("length-error").to(sinkTopic[1], Produced.with(Serdes.String(),Serdes.String()));
        
        return new KafkaStreams(builder.build(), config);
    }
}
