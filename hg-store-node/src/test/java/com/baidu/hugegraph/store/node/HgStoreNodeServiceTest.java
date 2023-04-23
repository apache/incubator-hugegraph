package com.baidu.hugegraph.store.node;

// import org.junit.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HgStore单元测试
 * 1、测试raft多副本入库
 * 2、测试快照同步
 * 3、测试副本增减
 * 4、测试单幅本关闭日志入库
 */
public class HgStoreNodeServiceTest {

    // @Test
    public void testRaft() {

    }

    String yml = "rocksdb:\n" + "  # rocksdb 使用的总内存大小\n" + "  total_memory_size: 32000000000\n" + "  max_background_jobs: 8\n" + "  max_subcompactions: 4\n" + "  target_file_size_multiplier: 4\n" + "  min_write_buffer_number_to_merge: 8\n" + "  target_file_size_base: 512000000";

    // @Test
    public void testYaml() throws InterruptedException, IOException {

        ExecutorService executor = new ThreadPoolExecutor(1000, 1000,
                10L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10000));
        CountDownLatch latch = new CountDownLatch(100);
        for(int i =0 ;i < 100; i++){

            executor.execute(()->{
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(executor);
                System.out.println(Thread.activeCount());
                latch.countDown();
            });

        }
        latch.await();
        System.in.read();
    }
}
