package com.addnewer.easylink.jdbc.sink;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author pinru
 * @version 1.0
 */
public class InsertOrUpdateJdbcBatchSerializer<T> extends TypeSerializer<InsertOrUpdateJdbcBatch<T>> {
    private final TypeSerializer<T> typeSerializer;
    public InsertOrUpdateJdbcBatchSerializer(final TypeSerializer<T> typeSerializer) {

        this.typeSerializer = typeSerializer;
    }


    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<InsertOrUpdateJdbcBatch<T>> duplicate() {
        return new InsertOrUpdateJdbcBatchSerializer<>(typeSerializer);
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> createInstance() {
        return new InsertOrUpdateJdbcBatch<>();
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> copy(final InsertOrUpdateJdbcBatch<T> from) {
        return from.clone();
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> copy(final InsertOrUpdateJdbcBatch<T> from, final InsertOrUpdateJdbcBatch<T> reuse) {
        return from.clone();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(final InsertOrUpdateJdbcBatch<T> record, final DataOutputView target) throws IOException {
        target.writeInt(record.insertBatch.size());
        target.writeInt(record.updateBatch.size());
        for (final T insertBatch : record.insertBatch) {
            typeSerializer.serialize(insertBatch, target);
        }
        for (final T updateBatch : record.updateBatch) {
            typeSerializer.serialize(updateBatch, target);
        }
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> deserialize(final DataInputView source) throws IOException {
        final int insertBatchSize = source.readInt();
        final int updateBatchSize = source.readInt();
        InsertOrUpdateJdbcBatch<T> res = new InsertOrUpdateJdbcBatch<>();
        final List<T> insertBatch = res.insertBatch;
        final List<T> updateBatch = res.updateBatch;
        for (int i = 0; i < insertBatchSize; i++) {
            insertBatch.add(typeSerializer.deserialize(source));
        }
        for (int i = 0; i < updateBatchSize; i++) {
            updateBatch.add(typeSerializer.deserialize(source));
        }
        return res;
    }

    @Override
    public InsertOrUpdateJdbcBatch<T> deserialize(final InsertOrUpdateJdbcBatch<T> reuse, final DataInputView source) throws IOException {

        return this.deserialize(source);
    }

    @Override
    public void copy(final DataInputView source, final DataOutputView target) throws IOException {
        final int insertBatchSize = source.readInt();
        target.writeInt(insertBatchSize);
        final int updateBatchSize = source.readInt();
        target.writeInt(updateBatchSize);
        for (int i = 0; i < insertBatchSize; i++) {
            typeSerializer.copy(source, target);
        }
        for (int i = 0; i < updateBatchSize; i++) {
            typeSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && obj.getClass() == this.getClass() && this.typeSerializer.equals(((InsertOrUpdateJdbcBatchSerializer) obj).typeSerializer);
    }

    @Override
    public int hashCode() {
        return typeSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<InsertOrUpdateJdbcBatch<T>> snapshotConfiguration() {
        return new InsertOrUpdateJdbcBatchSnapshot<>(()->this);
    }

    private static class InsertOrUpdateJdbcBatchSnapshot<T> extends SimpleTypeSerializerSnapshot<InsertOrUpdateJdbcBatch<T>> {

        /**
         * Constructor to create snapshot from serializer (writing the snapshot).
         *
         * @param serializerSupplier
         */
        public InsertOrUpdateJdbcBatchSnapshot(@Nonnull final Supplier<? extends TypeSerializer<InsertOrUpdateJdbcBatch<T>>> serializerSupplier) {
            super(serializerSupplier);
        }
    }


}
