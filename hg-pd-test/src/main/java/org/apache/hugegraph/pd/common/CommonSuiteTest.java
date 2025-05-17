package org.apache.hugegraph.pd.common;

import org.apache.hugegraph.pd.service.IdServiceTest;
import org.apache.hugegraph.pd.service.KvServiceTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
        PartitionUtilsTest.class,
        PartitionCacheTest.class,
        MetadataKeyHelperTest.class,
        KvServiceTest.class,
        HgAssertTest.class,
        KVPairTest.class,
        IdServiceTest.class
})

@Slf4j
public class CommonSuiteTest {


}