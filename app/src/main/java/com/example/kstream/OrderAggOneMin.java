package com.example.kstream;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.example.kstream.util.SerdesFactory;
import com.example.kstream.util.StreamUtil;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import ksql.orders;
import ksql.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderAggOneMin {

    static Logger logger = LoggerFactory.getLogger(OrderAggOneMin.class);
    public static void main(String[] argc) {
        Properties config;
        try {
            config = StreamUtil.getInitialProps("order-agg-one-min");
            KafkaStreams stream = OrderAggOneMin.buildJsonToAvroStream(config, "test.orders", "test.orders.add.cnt");
            logger.info(stream.toString());
            stream.start();
    
            Runtime.getRuntime().addShutdownHook(new Thread(stream::close));    
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static KafkaStreams buildJsonToAvroStream(final Properties config, final String sourceTopic,final String sinkTopic ) {
        SerdesFactory.setSerdesConfig(config);

        final StreamsBuilder builder = new StreamsBuilder();

        // add code here
        
        return new KafkaStreams(builder.build(), config);


    }
}
