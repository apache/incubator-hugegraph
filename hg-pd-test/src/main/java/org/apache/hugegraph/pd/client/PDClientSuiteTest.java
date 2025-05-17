package org.apache.hugegraph.pd.client;

import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
        PDClientTest.class,
        KvClientTest.class,
        DiscoveryClientTest.class
})

@Slf4j
public class PDClientSuiteTest {

}
