package com.addnewer.easylink.jdbc.sink;

import com.addnewer.easylink.jdbc.DatasourceProperty;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 提供vertica connection，内部使用的是druid(沿用重构前的)，可以替换成普通connection
 *
 * @author pinru
 * @version 1.0
 */
public class VerticaConnectionProvider implements JdbcConnectionProvider, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(VerticaConnectionProvider.class);

    private transient Connection connection;
    private static DataSource dataSource;
    private final DatasourceProperty bean;


    public VerticaConnectionProvider(DatasourceProperty bean) {
        this.bean = bean;
    }

    /**
     * 获得connection,目前是从Druid连接池
     *
     * @author pinru
     */
    private void resetConnection() throws SQLException {
        synchronized (VerticaConnectionProvider.class){
            if (dataSource == null) {
                dataSource = bean.toDataSource();
            }
        }
        connection = dataSource.getConnection();
    }

    @Nullable
    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return connection.isValid(500);
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (connection != null) {
            return connection;
        }
        resetConnection();
        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public Connection reestablishConnection() throws SQLException {
        closeConnection();
        resetConnection();
        return connection;
    }
}
