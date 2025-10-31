# BackendEntry到RocksDB映射详解

## 核心映射流程

### 1. BackendEntry → BackendColumn

```java
// VectorBackendEntry.columns() 返回List<BackendColumn>
@Override
public Collection<BackendColumn> columns() {
    List<BackendColumn> cols = new ArrayList<>();
    
    // 向量数据序列化为byte[]
    if (this.vector != null) {
        cols.add(BackendColumn.of(
            "vector".getBytes(),           // name: 列名
            this.serializeVector()         // value: 序列化后的向量数据
        ));
    }
    
    // 距离度量方式
    if (this.metricType != null) {
        cols.add(BackendColumn.of(
            "metric".getBytes(),
            this.metricType.getBytes()    // "L2" / "COSINE" / "DOT"
        ));
    }
    
    // 向量维度
    if (this.dimension != null) {
        cols.add(BackendColumn.of(
            "dimension".getBytes(),
            this.dimension.toString().getBytes()
        ));
    }
    
    return Collections.unmodifiableList(cols);
}
```

### 2. BackendColumn结构

```java
public class BackendColumn {
    public byte[] name;    // 列名（字节数组）
    public byte[] value;   // 列值（字节数组）
    
    public static BackendColumn of(byte[] name, byte[] value) {
        BackendColumn col = new BackendColumn();
        col.name = name;
        col.value = value;
        return col;
    }
}
```

## RocksDB操作映射

### 3. Action → RocksDB操作

```java
// RocksDBStore.mutate(BackendMutation mutation)
public void mutate(BackendMutation mutation) {
    for (HugeType type : mutation.types()) {
        RocksDBSessions.Session session = this.session(type);
        for (Iterator<BackendAction> it = mutation.mutation(type); it.hasNext(); ) {
            BackendAction item = it.next();
            BackendEntry entry = item.entry();
            
            // 根据Action类型调用不同的操作
            switch (item.action()) {
                case INSERT:
                    table.insert(session, entry);
                    break;
                case APPEND:
                    table.append(session, entry);  // 等同于insert
                    break;
                case DELETE:
                    table.delete(session, entry);
                    break;
                case ELIMINATE:
                    table.eliminate(session, entry);  // 等同于delete
                    break;
            }
        }
    }
}
```

### 4. 具体的RocksDB操作

```java
// RocksDBTable.insert() - 写入操作
public void insert(RocksDBSessions.Session session, BackendEntry entry) {
    for (BackendColumn col : entry.columns()) {
        // 对每个BackendColumn执行put操作
        session.put(
            this.table(),      // 表名（Column Family）
            col.name,          // key: 列名的字节数组
            col.value          // value: 列值的字节数组
        );
    }
}

// RocksDBTable.delete() - 删除操作
public void delete(RocksDBSessions.Session session, BackendEntry entry) {
    if (entry.columns().isEmpty()) {
        // 如果没有列，删除整个entry
        session.delete(this.table(), entry.id().asBytes());
    } else {
        // 删除指定的列
        for (BackendColumn col : entry.columns()) {
            session.delete(this.table(), col.name);
        }
    }
}
```

### 5. Session操作 → WriteBatch

```java
// RocksDBStdSessions.Session.put()
public void put(String table, byte[] key, byte[] value) {
    try (OpenedRocksDB.CFHandle cf = cf(table)) {
        // 添加到WriteBatch（不立即写入）
        this.batch.put(cf.get(), key, value);
    } catch (RocksDBException e) {
        throw new BackendException(e);
    }
}

// RocksDBStdSessions.Session.delete()
public void delete(String table, byte[] key) {
    try (OpenedRocksDB.CFHandle cf = cf(table)) {
        // 添加到WriteBatch（不立即写入）
        this.batch.delete(cf.get(), key);
    } catch (RocksDBException e) {
        throw new BackendException(e);
    }
}

// RocksDBStdSessions.Session.commit() - 批量提交
public Integer commit() {
    int count = this.batch.count();
    if (count <= 0) {
        return 0;
    }
    
    try {
        // 一次性写入所有操作到RocksDB
        rocksdb().write(this.writeOptions, this.batch);
    } catch (RocksDBException e) {
        throw new BackendException(e);
    }
    
    // 清空batch
    this.batch.clear();
    return count;
}
```

## 向量索引的具体映射示例

### 写入向量索引

```
VectorBackendEntry {
    type: VECTOR_INDEX
    id: indexId_1
    subId: vertexId_1
    vectorId: "vertexId_1"
    vector: [0.1, 0.2, 0.3, ...]
    metricType: "L2"
    dimension: 768
}

↓ entry.columns()

BackendColumn[] {
    {name: "vector".getBytes(),    value: [序列化后的float[]]},
    {name: "metric".getBytes(),    value: "L2".getBytes()},
    {name: "dimension".getBytes(), value: "768".getBytes()}
}

↓ session.put(table, key, value)

WriteBatch {
    put(CF_VECTOR_INDEX, "vector".getBytes(),    [序列化后的float[]]),
    put(CF_VECTOR_INDEX, "metric".getBytes(),    "L2".getBytes()),
    put(CF_VECTOR_INDEX, "dimension".getBytes(), "768".getBytes())
}

↓ rocksdb.write()

RocksDB {
    CF_VECTOR_INDEX: {
        "vector"    → [序列化后的float[]],
        "metric"    → "L2",
        "dimension" → "768"
    }
}
```

### 删除向量索引

```
VectorBackendEntry {
    type: VECTOR_INDEX
    id: indexId_1
    vector: []  // 空数组表示删除
}

↓ entry.columns()

BackendColumn[] {
    {name: "vector".getBytes(), value: []}
}

↓ session.delete(table, key)

WriteBatch {
    delete(CF_VECTOR_INDEX, "vector".getBytes())
}

↓ rocksdb.write()

RocksDB {
    CF_VECTOR_INDEX: {
        "vector" → [已删除]
    }
}
```

## 关键点总结

| 步骤 | 输入 | 输出 | 说明 |
|------|------|------|------|
| 1 | VectorBackendEntry | List<BackendColumn> | 调用columns()方法 |
| 2 | BackendColumn | (table, key, value) | 提取name和value |
| 3 | Action类型 | insert/delete | 决定操作类型 |
| 4 | (table, key, value) | WriteBatch | 添加到批处理队列 |
| 5 | WriteBatch | RocksDB | 一次性提交所有操作 |

## 性能特点

- **批处理**：所有操作先加入WriteBatch，最后一次性提交
- **原子性**：WriteBatch中的所有操作要么全部成功，要么全部失败
- **效率**：减少RocksDB的写入次数，提高吞吐量

## 详细对照表

### 表1: BackendEntry字段 → BackendColumn映射

| BackendEntry字段 | 类型 | BackendColumn.name | BackendColumn.value | 说明 |
|-----------------|------|------------------|-------------------|------|
| vector | float[] | "vector" | 序列化后的byte[] | 向量数据 |
| metricType | String | "metric" | "L2"/"COSINE"/"DOT" | 距离度量 |
| dimension | int | "dimension" | "768" | 向量维度 |
| id | Id | (entry.id()) | (col.name前缀) | 索引ID |
| subId | Id | (entry.subId()) | (col.name前缀) | vertexId |

### 表2: Action类型 → RocksDB操作映射

| Action | 表方法 | Session方法 | WriteBatch操作 | 说明 |
|--------|--------|-----------|--------------|------|
| INSERT | insert() | put() | batch.put() | 插入新数据 |
| APPEND | append() | put() | batch.put() | 追加数据（等同INSERT） |
| DELETE | delete() | delete() | batch.delete() | 删除数据 |
| ELIMINATE | eliminate() | delete() | batch.delete() | 消除数据（等同DELETE） |
| UPDATE_IF_PRESENT | updateIfPresent() | put() | batch.put() | 存在时更新 |
| UPDATE_IF_ABSENT | updateIfAbsent() | put() | batch.put() | 不存在时更新 |

