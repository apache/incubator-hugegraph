package core;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.metric.HgMetricService;
import com.baidu.hugegraph.store.metric.SystemMetricService;

public class MetricServiceTest {

    private SystemMetricService service;
    private HgMetricService hgService;

    @Before
    public void setUp() {
        service = new SystemMetricService();
        HgStoreEngine instance = HgStoreEngine.getInstance();
        service.setStoreEngine(instance);
        hgService = HgMetricService.getInstance().setHgStoreEngine(instance);
    }

    @Test
    public void testGetStorageEngine() {
        HgStoreEngine result = service.getStorageEngine();
    }

    @Test
    public void testGetSystemMetrics() {
        try{
            Map<String, Long> systemMetrics = service.getSystemMetrics();
            Thread.sleep(1000);
            systemMetrics = service.getSystemMetrics();
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetHgMetrics() {
        // Setup
        Metapb.StoreStats.Builder systemMetrics = hgService.getMetrics();
    }
}
