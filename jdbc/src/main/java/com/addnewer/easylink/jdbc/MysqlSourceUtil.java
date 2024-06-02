package com.addnewer.easylink.jdbc;

import com.addnewer.easylink.jdbc.cdc.MysqlSourceProperty;
import com.addnewer.easylink.jdbc.cdc.PojoDebeziumDeserialization;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author pinru
 * @version 1.0
 */
public class MysqlSourceUtil {
    public static <T> MySqlSource<Tuple2<String,T>> mySqlSourcePojo(MysqlSourceProperty property, Class<T> clazz) {
        return MySqlSource
                .<Tuple2<String,T>>builder()
                .hostname(property.getHostname())
                .port(property.getPort())
                .username(property.getUsername())
                .password(property.getPassword())
                .connectionPoolSize(property.getConnectPoolSize())
                .startupOptions(StartupOptions.initial())
                .databaseList(property.getDatabases())
                .tableList(property.getTables())
                .scanNewlyAddedTableEnabled(property.isScanNew())
                .deserializer(new PojoDebeziumDeserialization<>(clazz))
                .serverId(property.getServerId())
                .build();

    }
    public static MySqlSource<String> mySqlSourceJson(MysqlSourceProperty property) {
        return MySqlSource
                .<String>builder()
                .hostname(property.getHostname())
                .port(property.getPort())
                .username(property.getUsername())
                .password(property.getPassword())
                .connectionPoolSize(property.getConnectPoolSize())
                .startupOptions(StartupOptions.initial())
                .databaseList(property.getDatabases())
                .tableList(property.getTables())
                .scanNewlyAddedTableEnabled(property.isScanNew())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverId(property.getServerId())
                .build();

    }
}
