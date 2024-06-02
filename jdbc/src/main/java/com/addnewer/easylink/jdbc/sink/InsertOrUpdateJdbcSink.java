package com.addnewer.easylink.jdbc.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * jdbc事务写入的SinkFunction,使用二阶段提交实现,只支持insert和update操作
 *
 * @author pinru
 * @version 1.0
 */
public class InsertOrUpdateJdbcSink<T> extends TwoPhaseCommitSinkFunction<Tuple2<String, T>, InsertOrUpdateJdbcBatch<T>, Void> {

    private final JdbcConnectionProvider connectionProvider;
    private final String insertSQL;
    private final String updateSQL;

    private final JdbcStatementBuilder<T> insertSetter;
    private final JdbcStatementBuilder<T> updateSetter;

    public InsertOrUpdateJdbcSink(JdbcConnectionProvider connectionProvider, String insertSQL, String updateSQL, JdbcStatementBuilder<T> insertSetter, JdbcStatementBuilder<T> updateSetter, Class<T> clazz) {
        super(new InsertOrUpdateJdbcBatchSerializer<>(PojoTypeInfo.of(clazz).createSerializer(new ExecutionConfig())),
                VoidSerializer.INSTANCE);
        this.connectionProvider = connectionProvider;
        this.insertSQL = insertSQL;
        this.updateSQL = updateSQL;
        this.insertSetter = insertSetter;
        this.updateSetter = updateSetter;
    }


    @Override
    protected void invoke(InsertOrUpdateJdbcBatch<T> transaction, Tuple2<String, T> value, Context context) throws Exception {
        if(transaction.addToBatch(value)){
            preCommit(transaction);
        }
    }

    @Override
    protected InsertOrUpdateJdbcBatch<T> beginTransaction() throws Exception {
        return new InsertOrUpdateJdbcBatch<>();
    }

    @Override
    protected void preCommit(InsertOrUpdateJdbcBatch<T> txn) throws Exception {
        connectionProvider.getOrEstablishConnection();
        if (!connectionProvider.isConnectionValid()) {
            connectionProvider.reestablishConnection();
        }
        Connection connection = connectionProvider.getConnection();
        connection.setAutoCommit(false);
        try {
            PreparedStatement insertPs = connection.prepareStatement(insertSQL);
            PreparedStatement updatePs = connection.prepareStatement(updateSQL);
            txn.executeBatch(insertPs, updatePs, insertSetter, updateSetter);
            insertPs.close();
            updatePs.close();
        } catch (SQLException e) {
            connection.rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commit(InsertOrUpdateJdbcBatch<T> txn) {
        Connection connection = connectionProvider.getConnection();
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        connectionProvider.getOrEstablishConnection();
    }

    @Override
    protected void abort(InsertOrUpdateJdbcBatch<T> txn) {
        Connection connection = connectionProvider.getConnection();
        if(connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        connectionProvider.closeConnection();
    }

}

