package client;

import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
//        GraphStoreClientTest.class
        HgKvStoreTest.class,
        HgStoreClientTest.class,
        HgStoreNodeStateTest.class,
        ChangeShardNumTest.class,
        HgSessionManagerRaftPDTest.class,
        HgAssertTest.class,
        HgPairTest.class
})

@Slf4j
public class ClientSuiteTest {

}
