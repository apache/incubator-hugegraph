package raftcore;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    BytesCarrierTest.class,
    ZeroByteStringHelperTest.class
})
public class RaftSuiteTest {

}
