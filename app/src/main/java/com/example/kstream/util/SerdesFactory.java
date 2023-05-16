package com.example.kstream.util;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class SerdesFactory {
    private static Map<String,String> serdeConfig;
    /**
     * Return a serdes for requested class
     * @param <T> Class of requested Serdes
     * @return a serdes for Requested Class
     */
    public static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdes() {        
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<T>();
        serde.configure(getSerdesConfig(), false);
        return serde;
    }

    public static void setSerdesConfig(Properties config){
        serdeConfig = new HashMap<String,String>();
        for (final String name: config.stringPropertyNames())
            serdeConfig.put(name, config.getProperty(name));
    }

    public static Map<String,String> getSerdesConfig(){
        return serdeConfig;
    }
 }
 