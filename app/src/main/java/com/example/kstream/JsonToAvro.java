package com.example.kstream;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.example.kstream.util.SerdesFactory;
import com.example.kstream.util.StreamUtil;
import ksql.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToAvro {

    static Logger logger = LoggerFactory.getLogger(JsonToAvro.class);
    public static void main(String[] argc) {
        Properties config;
        try {
            config = StreamUtil.getInitialProps("json-to-avro");
            KafkaStreams stream = JsonToAvro.buildJsonToAvroStream(config, "product-json2", "json-avro-product");
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
        JSONParser parser = new JSONParser();

        // read the source stream
        final KStream<String, String> jsonToAvroStream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), Serdes.String()));
        jsonToAvroStream.mapValues( v -> {
            product product = new product();
            try {
                JSONObject jsonObject = (JSONObject) parser.parse(v);
                product.setDescription((String)jsonObject.get("description"));
                product.setId((Long)jsonObject.get("id"));
                product.setName((String)jsonObject.get("name"));
                product.setPrice((Double)jsonObject.get("price"));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            return product;

        }).filter((k, v) -> v != null).to(sinkTopic,Produced.with(Serdes.String(), SerdesFactory.<product>getSerdes()));

        return new KafkaStreams(builder.build(), config);
    }
}
