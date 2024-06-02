package com.addnewer.easylink.jdbc.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc事务写入的SinkFunction,使用二阶段提交实现
 *
 * @author pinru
 * @version 1.0
 */
public class TransactionJdbcSink<T> extends TwoPhaseCommitSinkFunction<T, List<T>, Void> {

    private final JdbcConnectionProvider connectionProvider;
    private final String sql;

    private final JdbcStatementBuilder<T> statementBuilder;

    public TransactionJdbcSink(JdbcConnectionProvider connectionProvider, String sql, JdbcStatementBuilder<T> statementBuilder, Class<T> clazz) {
        super(new ListSerializer<>(TypeInformation.of(clazz).createSerializer(new ExecutionConfig())),
                VoidSerializer.INSTANCE);

        this.connectionProvider = connectionProvider;
        this.sql = sql;
        this.statementBuilder = statementBuilder;
    }

    /**
     * @param transaction
     * @param value
     * @param context
     * @author pinru
     */
    @Override
    protected void invoke(List<T> transaction, T value, Context context) throws Exception {
        if (transaction.size() > 20000) {
            preCommit(transaction);
        }
        transaction.add(value);
    }

    @Override
    protected List<T> beginTransaction() throws Exception {
        return new ArrayList<>();
    }

    @Override
    protected void preCommit(List<T> txn) throws Exception {
        connectionProvider.getOrEstablishConnection();
        if (!connectionProvider.isConnectionValid()) {
            connectionProvider.reestablishConnection();
        }
        Connection connection = connectionProvider.getConnection();
        connection.setAutoCommit(false);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (final T t : txn) {
                statementBuilder.accept(ps, t);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
        txn.clear();

    }

    @Override
    protected void commit(List<T> txn) {

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
        connectionProvider.reestablishConnection();
    }

    @Override
    protected void abort(List<T> txn) {
        Connection connection = connectionProvider.getConnection();
        if (connection != null) {
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

