package com.baidu.hugegraph.store.node;

import com.alipay.remoting.util.StringUtils;
import com.baidu.hugegraph.store.node.listener.ContextClosedListener;
import com.baidu.hugegraph.store.node.listener.PdConfigureListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 *
 */
@SpringBootApplication
public class StoreNodeApplication {

    //TODO Is this OK?
    private final AppShutdownHook shutdownHook = new AppShutdownHook(Thread.currentThread());

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        // 设置solt用到的日志位置
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs");
        }
        System.setProperty("com.alipay.remoting.client.log.level", "WARN");
        if (System.getProperty("bolt.channel_write_buf_low_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_low_water_mark", Integer.toString(4 * 1024 * 1024));
        if (System.getProperty("bolt.channel_write_buf_high_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_high_water_mark", Integer.toString(8 * 1024 * 1024));
        SpringApplication application = new SpringApplication(StoreNodeApplication.class);
        PdConfigureListener listener = new PdConfigureListener();
        ContextClosedListener closedListener = new ContextClosedListener();
        application.addListeners(listener);
        application.addListeners(closedListener);
        ConfigurableApplicationContext context = application.run();
        listener.setContext(context);
        System.out.println("StoreNodeApplication started.");
    }
}
