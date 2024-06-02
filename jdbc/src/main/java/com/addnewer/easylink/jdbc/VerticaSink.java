package com.addnewer.easylink.jdbc;

import com.addnewer.easylink.jdbc.sink.InsertOrUpdateJdbcSink;
import com.addnewer.easylink.jdbc.sink.TransactionJdbcSink;
import com.addnewer.easylink.jdbc.sink.VerticaConnectionProvider;
import com.addnewer.easylink.jdbc.sink.VerticaUpsertExecutor;
import com.addnewer.easylink.utils.PojoUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 写入vertica的工具类，有两种实现，upsert和 transaction，实测transaction性能好一些，因为vertica的upsert需要全表扫描，推荐使用transaction。
 *
 * @author pinru
 * @version 1.0
 */
public class VerticaSink {
    /**
     * vertica upsert写入，至少一次语义
     *
     * @param jdbcConnectionProvider 提供jdbc connection
     * @param batchOptions           控制每批数据写入条件
     * @param executor               执行器
     * @return {@link GenericJdbcSinkFunction<T>}
     * @author pinru
     */
    public static <T> GenericJdbcSinkFunction<T> upsertSink(JdbcConnectionProvider jdbcConnectionProvider, JdbcExecutionOptions batchOptions, VerticaUpsertExecutor<T> executor) {
        return new GenericJdbcSinkFunction<>(new JdbcOutputFormat<>(
                jdbcConnectionProvider,
                batchOptions,
                new VerticaUpsertExecutorFactory<>(executor),
                JdbcOutputFormat.RecordExtractor.identity()
        ));
    }

    /**
     * vertica upsert写入，至少一次语义。简化使用版
     *
     * @param bean  jdbc属性bean
     * @param table 表名
     * @param clazz POJO的类
     * @return {@link GenericJdbcSinkFunction<T>}
     * @author pinru
     */
    public static <T> GenericJdbcSinkFunction<T> upsertSink(DatasourceProperty bean, String table, Class<?> clazz) {
        String sql = VerticaUpsertExecutor.upsertSqlFormat(table, clazz);
        return upsertSink(new VerticaConnectionProvider(bean),
                JdbcExecutionOptions.builder().withBatchSize(500000).withBatchIntervalMs(Time.seconds(10).toMilliseconds()).build(),
                new ClassUpsertExecutor<>(sql, clazz)
        );
    }

    /**
     * 阶段提交+事务的vertica sink，保证精确一次语义，要求数据类型是Pojo
     *
     * @param jdbcConnectionProvider Jdbc链接提供器 {@link JdbcConnectionProvider}
     * @param sql                    Sql模板
     * @param statementBuilder       sql占位符填充
     * @return {@link TransactionJdbcSink <T>}
     * @author pinru
     */
    public static <T> TransactionJdbcSink<T> transactionSink(JdbcConnectionProvider jdbcConnectionProvider, String sql, JdbcStatementBuilder<T> statementBuilder, Class<T> clazz) {
        return new TransactionJdbcSink<>(jdbcConnectionProvider, sql, statementBuilder, clazz);
    }

    /**
     * 阶段提交+事务的vertica sink，保证精确一次语义，要求数据类型是Pojo，简化使用板
     *
     * @param bean  jdbc属性bean
     * @param table 表名
     * @param clazz POJO的类
     * @return {@link TransactionJdbcSink<T>}
     * @author pinru
     */
    public static <T> TransactionJdbcSink<T> transactionSink(DatasourceProperty bean, String table, Class<?> clazz) {
        return new TransactionJdbcSink<>(new VerticaConnectionProvider(bean), InsertSetter.insertSqlFormat(table, clazz), new InsertSetter<>(clazz), (Class<T>) clazz);
    }


    /**
     * 二阶段提交+事务的vertica sink，保证精确一次语义，要求数据流的类型是{@link Tuple2},tuple第一个是insert或update，第二个是POJO
     *
     * @param provider     Jdbc链接提供器 {@link JdbcConnectionProvider}
     * @param insertSql    insertSql模板
     * @param updateSql    updateSql模板
     * @param insertSetter insert占位符填充
     * @param updateSetter update占位符填充
     * @return {@link InsertOrUpdateJdbcSink <T>}
     * @author pinru
     */
    public static <T> InsertOrUpdateJdbcSink<T> insertOrUpdateSink(JdbcConnectionProvider provider, String insertSql, String updateSql, JdbcStatementBuilder<T> insertSetter, JdbcStatementBuilder<T> updateSetter, Class<?> clazz) {
        return new InsertOrUpdateJdbcSink<>(provider, insertSql, updateSql, insertSetter, updateSetter, (Class<T>) clazz);
    }

    /**
     * 二阶段提交+事务的vertica sink，保证精确一次语义,简化使用版。
     *
     * @param bean  jdbc属性bean
     * @param table 表名
     * @param clazz POJO的类
     * @return {@link InsertOrUpdateJdbcSink<T>}
     * @author pinru
     */
    public static <T> InsertOrUpdateJdbcSink<T> insertOrUpdateSink(DatasourceProperty bean, String table, Class<?> clazz) {

        return new InsertOrUpdateJdbcSink<>(new VerticaConnectionProvider(bean), InsertSetter.insertSqlFormat(table, clazz), UpdateSetter.updateSqlFormat(table, clazz), new InsertSetter<>(clazz), new UpdateSetter<>(clazz), (Class<T>) clazz);
    }


    public static class ClassUpsertExecutor<T> extends VerticaUpsertExecutor<T> implements Serializable {
        public ClassUpsertExecutor(String sql, Class<?> clazz) {
            super(sql, new UpsertSetter<>(clazz));
        }
    }


    public static class UpsertSetter<T> implements JdbcStatementBuilder<T>, Serializable {

        private transient Field id;
        private transient List<Field> fieldsWithoutId;
        private final Class<?> clazz;

        private void init() {
            if (id == null) {
                try {
                    id = clazz.getDeclaredField("id");
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException("没有id字段！ ", e);
                }
            }
            if (fieldsWithoutId == null) {
                Field[] fields = clazz.getDeclaredFields();
                fieldsWithoutId = Arrays
                        .stream(fields)
                        .filter(f -> !"id".equalsIgnoreCase(f.getName()))
                        .collect(Collectors.toList());
            }
        }

        public UpsertSetter(Class<?> clazz) {
            this.clazz = clazz;
            init();
        }


        @Override
        public void accept(PreparedStatement ps, T t) throws SQLException {
            init();
            int index = 1;
            try {
                for (int r = 2; r > 0; r--) {
                    id.setAccessible(true);
                    ps.setLong(index++, (Long) id.get(t));
                    for (Field field : fieldsWithoutId) {
                        ps.setObject(index++,field.get(t));
                    }
                }

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static class VerticaUpsertExecutorFactory<T> implements JdbcOutputFormat.StatementExecutorFactory<JdbcBatchStatementExecutor<T>>, Serializable {
        private final VerticaUpsertExecutor<T> executor;

        public VerticaUpsertExecutorFactory(final VerticaUpsertExecutor<T> executor) {
            this.executor = executor;
        }

        @Override
        public JdbcBatchStatementExecutor<T> apply(final RuntimeContext context) {
            return executor;
        }
    }

    private static class InsertSetter<T> implements JdbcStatementBuilder<T> {

        private transient Field id;
        private transient List<Field> fieldsWithoutId;
        private final Class<?> clazz;

        private void init() {
            if (id == null) {
                try {
                    id = clazz.getDeclaredField("id");
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException("没有id字段！ ", e);
                }
            }
            if (fieldsWithoutId == null) {
                Field[] fields = clazz.getDeclaredFields();
                fieldsWithoutId = Arrays
                        .stream(fields)
                        .filter(f -> !"id".equalsIgnoreCase(f.getName()))
                        .collect(Collectors.toList());
            }
        }

        public InsertSetter(Class<?> clazz) {
            this.clazz = clazz;
            init();
        }


        @Override
        public void accept(PreparedStatement ps, T t) throws SQLException {
            init();
            int index = 1;
            try {
                id.setAccessible(true);
                ps.setLong(index++, (Long) id.get(t));
                for (Field field : fieldsWithoutId) {
                    ps.setObject(index++,field.get(t));
                }

            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        private static String insertSqlFormat(String table, Class<?> clazz) {
            String template = "INSERT into %s (%s) VALUES (%s)";

            Field[] fields = clazz.getDeclaredFields();
            List<String> columns = Arrays
                    .stream(fields)
                    .map(f -> PojoUtil.camelToSnake(f.getName()))
                    .collect(Collectors.toList());
            String columnsStr = String.join(",", columns);
            String valuesStr = String.join(",", Collections.nCopies(columns.size(), "?"));

            return String.format(template, table, columnsStr, valuesStr);
        }
    }

    private static class UpdateSetter<T> implements JdbcStatementBuilder<T> {

        private transient Field id;
        private transient List<Field> fieldsWithoutId;
        private final Class<?> clazz;

        private void init() {
            if (id == null) {
                try {
                    id = clazz.getDeclaredField("id");
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException("没有id字段！ ", e);
                }
            }
            if (fieldsWithoutId == null) {
                Field[] fields = clazz.getDeclaredFields();
                fieldsWithoutId = Arrays
                        .stream(fields)
                        .filter(f -> !"id".equalsIgnoreCase(f.getName()))
                        .collect(Collectors.toList());
            }
        }

        public UpdateSetter(Class<?> clazz) {
            this.clazz = clazz;
            init();
        }


        @Override
        public void accept(PreparedStatement ps, T t) throws SQLException {
            init();
            int index = 1;
            try {
                for (Field field : fieldsWithoutId) {
                    ps.setObject(index++,field.get(t));
                }
                id.setAccessible(true);
                ps.setLong(index++, (Long) id.get(t));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        private static String updateSqlFormat(String table, Class<?> clazz) {
            String template = "UPDATE %s SET %s WHERE id=?";
            Field id = null;
            try {
                id = clazz.getDeclaredField("id");
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("没有id字段！ ", e);
            }

            String updateColumn = "%s =?";
            Field[] fields = clazz.getDeclaredFields();
            List<String> columns = Arrays
                    .stream(fields)
                    .filter(f -> !"id".equalsIgnoreCase(f.getName()))
                    .map(f -> PojoUtil.camelToSnake(f.getName()))
                    .collect(Collectors.toList());
            List<String> updateStrings = columns
                    .stream()
                    .map(f -> String.format(updateColumn, f))
                    .collect(Collectors.toList());
            String updateJoin = String.join(",", updateStrings);
            return String.format(template, table, updateJoin);
        }
    }
}
