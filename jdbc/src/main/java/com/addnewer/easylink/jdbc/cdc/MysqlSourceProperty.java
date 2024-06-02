package com.addnewer.easylink.jdbc.cdc;

import com.addnewer.easylink.auto.Prefix;
import lombok.Data;

/**
 * @author pinru
 * @version 1.0
 */
@Data
@Prefix("flink.cdc.")
public class MysqlSourceProperty {
    String hostname;
    int port = 3306;
    String username;
    String password;
    String serverId;
    String[] databases;
    String[] tables;
    boolean scanNew = false;
    int connectPoolSize = 10;
}
