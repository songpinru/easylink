package com.addnewer;

import com.addnewer.easylink.api.*;
import com.addnewer.easylink.jdbc.DatasourceProperty;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author pinru
 * @version 1.0
 */
@Configuration
public class ExampleConfig {


    @Source
    DataStream<String> source(StreamExecutionEnvironment env, @Name("aa") DatasourceProperty property) {
        return env.fromElements("ss", "aa", "bb", "cc");
    }

    @Bean
    public SinkFunction<String> sink() {
        return new PrintSinkFunction<>();
    }
}
