package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchState;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.apache.commons.lang3.StringUtils;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;

public class KvClientTest extends BaseClientTest {

    private KvClient<WatchResponse> client;

    @Before
    public void setUp() {
        client = new KvClient<>(getPdConfig());
    }

    String key = "key";
    String value = "value";

    @Test
    public void testPutAndGet() throws Exception {
        // Run the test
        try {
            client.put(key, value);
            // Run the test
            KResponse result = client.get(key);

            // Verify the results
            assertThat(result.getValue()).isEqualTo(value);
            client.delete(key);
            result = client.get(key);
            assertThat(StringUtils.isEmpty(result.getValue()));
            client.deletePrefix(key);
            client.put(key + "1", value);
            client.put(key + "2", value);
            ScanPrefixResponse response = client.scanPrefix(key);
            assertThat(response.getKvsMap().size() == 2);
            client.putTTL(key + "3", value, 1000);
            client.keepTTLAlive(key + "3");
            final Consumer<WatchResponse> mockConsumer = mock(Consumer.class);

            // Run the test
            client.listen(key + "3", mockConsumer);
            client.listenPrefix(key + "4", mockConsumer);
            WatchResponse r = WatchResponse.newBuilder().addEvents(
                                                   WatchEvent.newBuilder().setCurrent(
                                                           WatchKv.newBuilder().setKey(key).setValue("value")
                                                                  .build()).setType(WatchType.Put).build())
                                           .setClientId(0L)
                                           .setState(WatchState.Starting)
                                           .build();
            client.getWatchList(r);
            client.getWatchMap(r);
            client.lock(key, 3000L);
            client.isLocked(key);
            client.unlock(key);
            client.lock(key, 3000L);
            client.keepAlive(key);
            client.close();
        } catch (Exception e) {

        }
    }
}
