package com.addnewer.easylink.kafka;

import com.addnewer.easylink.auto.Prefix;
import lombok.Data;

/**
 * @author pinru
 * @version 1.0
 */
@Data
@Prefix("flink.kafka.source")
public class KafkaSourceProperty {
    String bootstrapServers;
    String[] topics;
    String groupId;
}
