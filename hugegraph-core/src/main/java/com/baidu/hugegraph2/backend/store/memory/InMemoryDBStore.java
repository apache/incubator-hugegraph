package com.baidu.hugegraph2.backend.store.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.query.SliceQuery;
import com.baidu.hugegraph2.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryDBStore.class);

    // TODO get from configuration
    private static final String SCHEMATYPE_COLUME = "_schema";

    private final String name;
    private final ConcurrentNavigableMap<Object, BackendEntry> store;

    public InMemoryDBStore(final String name){
        logger.info("init: " + name);
        this.name = name;
        this.store = new ConcurrentSkipListMap<Object, BackendEntry>();
    }

    @Override
    public List<BackendEntry> getSlice(SliceQuery query) {

        List<BackendEntry> entries = new ArrayList<BackendEntry>();
        this.store.forEach((Object key, BackendEntry item) ->{

            // TODO: Compatible with BackendEntry
            TextBackendEntry entry = (TextBackendEntry) item;

            query.conditions.forEach((quality,value)->{
                boolean isContain = false;
                if(entry.contains(quality) && entry.column(quality).equals(value)){
                    isContain = true;
                }else {
                    isContain =false;
                }
                if(isContain){
                    entries.add(entry);
                }
            });

        });
        return entries;
    }

    @Override
    public void mutate(Collection<BackendEntry> additions, Set<Id> deletions) {
        logger.info("mutate(): additions {}, deletions {}",
                additions.size(), deletions.size());

        additions.forEach((entry)->{
            logger.info("[store {}] add entry: {}", this.name, entry);
            this.store.put(entry.id(),entry);
        });

        deletions.forEach((k)->{
            logger.info("[store {}] remove id: {}", this.name, k.asString());
            this.store.remove(k);
        });
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void clear() {
        logger.info("clear()");
        this.store.clear();
    }

    @Override
    public void close() throws BackendException {
        logger.info("close()");
    }

    @Override
    public void beginTx() {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitTx() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollbackTx() {
        // TODO Auto-generated method stub

    }
}
