package com.addnewer;

import com.addnewer.easylink.api.AppSource;
import com.addnewer.easylink.api.FlinkJob;
import com.addnewer.easylink.api.Name;
import com.addnewer.easylink.api.Value;
import com.addnewer.easylink.jdbc.DatasourceProperty;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author pinru
 * @version 1.0
 */
public class ExampleJob implements FlinkJob {
    private final AppSource<String> appSource;
    private final SinkFunction<String> sinkFunction;

    @Value("ss")
    int aa;

    public ExampleJob(AppSource<String> appSource, SinkFunction<String> sinkFunction, @Name("ss") String ss, @Name("aa") DatasourceProperty property) {
        System.out.println(property);
        this.appSource = appSource;
        this.sinkFunction = sinkFunction;
    }

    @Override
    public void run() throws Exception {
        System.out.println(aa);
        // appSource.getDataStream().addSink(sinkFunction);
    }
}
