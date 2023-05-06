package org.apache.hugegraph.pd.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
        RestApiTest.class,
        ConfigServiceTest.class,
        IdServiceTest.class,
        KvServiceTest.class,
        LogServiceTest.class,
        StoreServiceTest.class,
        StoreNodeServiceNewTest.class,
        StoreMonitorDataServiceTest.class,
        TaskScheduleServiceTest.class,
        PartitionServiceTest.class
})

@Slf4j
public class ServerSuiteTest {
}
