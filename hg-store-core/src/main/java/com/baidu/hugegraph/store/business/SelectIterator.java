package com.baidu.hugegraph.store.business;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.SerialEnum;

/**
 * @author zhangyingjie
 * @date 2022/9/23
 **/
public class SelectIterator implements ScanIterator {

    ScanIterator iter;
    Set<Integer> properties;

    public SelectIterator(ScanIterator iterator, List<Integer> properties) {
        this.iter = iterator;
        this.properties = new HashSet<>(properties);
    }

    public BackendColumn select(BackendColumn column) {
        int size;
        if (properties == null || (size = properties.size()) == 0) {
            return column;
        }
        byte[] name = column.name;
        byte[] value = column.value;
        BytesBuffer buffer = BytesBuffer.wrap(value);
        Id labelId = buffer.readId(); // label
        int bpSize = buffer.readVInt(); // property
        if (size == bpSize) {
            return column;
        }
        BytesBuffer allocate = BytesBuffer.allocate(8 + 16 * size);
        allocate.writeId(labelId);
        allocate.writeVInt(size);
        for (int i = 0; i < bpSize; i++) {
            int propertyId = buffer.readVInt();
            byte cat = buffer.read(); // cardinality and type
            byte code = BytesBuffer.getType(cat);
            DataType dataType = SerialEnum.fromCode(DataType.class, code);
            Object bpValue = buffer.readProperty(dataType);
            if (properties.contains(propertyId)) {
                allocate.writeVInt(propertyId);
                allocate.write(cat);
                allocate.writeProperty(dataType, bpValue);
            }
        }
        return BackendColumn.of(name, allocate.bytes());
    }

    @Override
    public boolean hasNext() {
        return this.iter.hasNext();
    }

    @Override
    public boolean isValid() {
        return this.iter.isValid();
    }

    @Override
    public BackendColumn next() {
        BackendColumn value = this.iter.next();
        return select(value);
    }

    @Override
    public long count() {
        return this.iter.count();
    }

    @Override
    public byte[] position() {
        return this.iter.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iter.seek(position);
    }

    @Override
    public void close() {
        this.iter.close();
    }
}
