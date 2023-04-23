package rocksdb;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.rocksdb.access.RocksDBFactory;
import com.baidu.hugegraph.rocksdb.access.RocksDBOptions;
import org.junit.After;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;

public class BaseRocksDbTest {
    @BeforeClass
    public static void init() {
        OptionSpace.register("rocksdb",
                "com.baidu.hugegraph.rocksdb.access.RocksDBOptions");
        RocksDBOptions.instance();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        HugeConfig hConfig  = new HugeConfig(configMap);
        RocksDBFactory rFactory  = RocksDBFactory.getInstance();
        rFactory.setHugeConfig(hConfig);

    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}
