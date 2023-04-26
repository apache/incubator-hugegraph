package com.baidu.hugegraph.store.meta;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.meta.base.GlobalMetaStore;
import com.baidu.hugegraph.store.options.MetadataOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class DeletedFileManager extends GlobalMetaStore {
    public DeletedFileManager(MetadataOptions options) {
        super(options);
    }

    public void load() {
        byte[] key = MetadataKeyHelper.getDeletedFilePrefix();
        List<RocksDBSession.BackendColumn> columns = scan(key);
        for (RocksDBSession.BackendColumn column : columns) {
            String filePath = new String(column.value, StandardCharsets.UTF_8);
            try {
                if (new File(filePath).exists()) {
                    FileUtils.deleteDirectory(new File(filePath));
                    log.warn("Delete legacy files {}", filePath);
                    removeDeletedFile(filePath);
                }
            } catch (IOException e) {
                log.error("Delete legacy files {} exception", filePath, e);
            }
        }
    }

    public void addDeletedFile(String path) {
        byte[] key = MetadataKeyHelper.getDeletedFileKey(path);
        put(key, path.getBytes(StandardCharsets.UTF_8));
    }

    public void removeDeletedFile(String path) {
        byte[] key = MetadataKeyHelper.getDeletedFileKey(path);
        delete(key);
    }


}
