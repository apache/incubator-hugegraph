package org.apache.hugegraph.pd.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.alipay.remoting.util.StringUtils;

/**
 * PD服务启动类
 */
@ComponentScan(basePackages = {"org.apache.hugegraph.pd"})
@SpringBootApplication
public class HugePDServer {
    public static void main(String[] args) {
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs");
            System.setProperty("com.alipay.remoting.client.log.level", "error");
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));
        SpringApplication.run(HugePDServer.class);
        System.out.println("Hugegraph-pd started.");
    }
}
