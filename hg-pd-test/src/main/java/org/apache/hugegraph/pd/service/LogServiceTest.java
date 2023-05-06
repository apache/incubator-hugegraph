package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.LogService;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.google.protobuf.Any;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LogServiceTest {

    private PDConfig mockPdConfig = BaseServerTest.getConfig();

    private LogService logServiceUnderTest;

    @Before
    public void setUp() {
        logServiceUnderTest = new LogService(mockPdConfig);
    }

    @Test
    public void testGetLog() throws Exception {
        logServiceUnderTest.insertLog("action", "message",
                                      Any.newBuilder().build());

        // Run the test
        final List<Metapb.LogRecord> result = logServiceUnderTest.getLog(
                "action", 0L, System.currentTimeMillis());

        // Verify the results
        Assert.assertEquals(result.size(), 1);
    }
}
