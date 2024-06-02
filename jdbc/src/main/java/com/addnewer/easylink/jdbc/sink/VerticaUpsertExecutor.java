package com.addnewer.easylink.jdbc.sink;

import com.google.common.base.CaseFormat;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
/**
 * vertica Upsert 实现，把数据存在内存中，按批处理
 * @author pinru
 * @version 1.0
 */
public class VerticaUpsertExecutor<T> implements JdbcBatchStatementExecutor<T> ,Serializable{

    /**
     * vertica upsert 语法，从官方文档获得
     */
    public static final String UPSERT_SQL= "MERGE INTO %s t USING (SELECT 1 ) d " +
            "ON (t.id=?) " +
            "WHEN MATCHED THEN UPDATE SET %s " +
            "WHEN NOT MATCHED THEN INSERT (%s) " +
            "VALUES (%s)";

    private final String sql;
    private final JdbcStatementBuilder<T> setter;

    private final List<T> batch=new ArrayList<>();
    private transient PreparedStatement ps;

    public VerticaUpsertExecutor(String sql, JdbcStatementBuilder<T> setter) {
        this.sql = sql;
        this.setter = setter;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
            ps = connection.prepareStatement(sql);
    }

    @Override
    public void addToBatch(T record) throws SQLException {
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        for (T record : batch) {
            setter.accept(ps,record);
            ps.addBatch();
        }
        ps.executeBatch();
        batch.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        ps.close();
    }

    public static String upsertSqlFormat(String table, Class<?> clazz) {
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
                .map(f -> CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE).convert(f.getName()))
                .collect(Collectors.toList());
        List<String> updateStrings = columns
                .stream()
                .map(f -> String.format(updateColumn, f))
                .collect(Collectors.toList());
        String updateJoin = String.join(",", updateStrings);

        String inserts = String.join(",", columns);
        String idColumn = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE).convert(id.getName());
        String insertJoin = idColumn + "," + inserts;

        List<String> values = Collections.nCopies(fields.length, "?");
        String valuesJoin = String.join(",", values);

        return String.format(UPSERT_SQL, table, updateJoin, insertJoin, valuesJoin);
    }
}
