package com.baidu.hugegraph.store.node.listener;

import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

import com.baidu.hugegraph.store.node.grpc.HgStoreStreamImpl;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2023/2/17
 **/
@Slf4j
public class ContextClosedListener implements ApplicationListener<ContextClosedEvent> {

    @Autowired
    HgStoreStreamImpl storeStream;
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        try {
            log.info("closing scan threads....");
            ThreadPoolExecutor executor = storeStream.getRealExecutor();
            if (executor != null) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {

                }
            }
        } catch (Exception e) {

        } finally {
            log.info("closed scan threads");
        }
    }
}
