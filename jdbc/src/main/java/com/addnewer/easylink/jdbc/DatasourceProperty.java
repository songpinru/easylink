package com.addnewer.easylink.jdbc;

import com.addnewer.easylink.auto.Prefix;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;

import javax.sql.DataSource;

/**
 * jdbc 数据源的参数
 *
 * @author pinru
 * @version 1.0
 */
@Data
@Prefix("flink.datasource.")
public class DatasourceProperty {

    String url;
    String user;
    String password;
    String driver;
    int initialSize = 1;
    int maxActive = 1;
    int minIdle = 1000 * 60 * 10;


    public DataSource toDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(this.getUrl());
        druidDataSource.setUsername(this.getUser());
        druidDataSource.setPassword(this.getPassword());
        druidDataSource.setDriverClassName(this.getDriver());
        druidDataSource.setInitialSize(this.getInitialSize());
        druidDataSource.setMaxActive(this.getMaxActive());
        druidDataSource.setMinIdle(this.getMinIdle());
        druidDataSource.setValidationQuery("select 1");
        return druidDataSource;
    }

}
