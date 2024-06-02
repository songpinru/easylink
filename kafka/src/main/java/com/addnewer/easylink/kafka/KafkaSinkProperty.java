package com.addnewer.easylink.kafka;

import com.addnewer.easylink.auto.Prefix;
import lombok.Data;

/**
 * @author pinru
 * @version 1.0
 */
@Data
@Prefix("flink.kafka.sink")
public class KafkaSinkProperty {
    String bootstrapServers;
    String topic;
    String acks;
    String retries;
    String batchSize;
}

