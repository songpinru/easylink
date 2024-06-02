package com.addnewer.easylink.jdbc.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 事务写入vertica的实现,把数据缓存在内存中,checkpoint时再提交,这个类会被序列化保存在状态中
 *
 * @author pinru
 * @version 1.0
 */
public class InsertOrUpdateJdbcBatch<T> implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    public List<T> insertBatch = new ArrayList<>();
    public List<T> updateBatch = new ArrayList<>();


    public boolean addToBatch(Tuple2<String, T> record) {
        String op = record.f0;
        if (op.equals("insert")) {
            insertBatch.add(record.f1);
        } else if (op.equals("update")) {
            updateBatch.add(record.f1);
        }
        return insertBatch.size()>20000||updateBatch.size()>5000;
    }

    public void executeBatch(PreparedStatement insertStatement, PreparedStatement updateStatement, JdbcStatementBuilder<T> insertSetter, JdbcStatementBuilder<T> updateSetter) throws SQLException {
        if (!insertBatch.isEmpty()) {
            for (T record : insertBatch) {
                insertSetter.accept(insertStatement, record);
                insertStatement.addBatch();
            }
            insertStatement.executeBatch();
            insertBatch.clear();
        }
        if (!updateBatch.isEmpty()) {
            for (T record : updateBatch) {
                updateSetter.accept(updateStatement, record);
                updateStatement.addBatch();
            }
            updateStatement.executeBatch();
            updateBatch.clear();
        }
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> clone() {
        InsertOrUpdateJdbcBatch<T> res = new InsertOrUpdateJdbcBatch<>();
        res.insertBatch.addAll(this.insertBatch);
        res.updateBatch.addAll(this.updateBatch);
        return res;
    }
}




