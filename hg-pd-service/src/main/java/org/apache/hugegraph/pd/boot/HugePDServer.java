package org.apache.hugegraph.pd.boot;

import com.alipay.remoting.util.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * PD服务启动类
 */
@ComponentScan(basePackages={"com.baidu.hugegraph.pd"})
@SpringBootApplication
public class HugePDServer {
    public static void main(String[] args) {
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs");
            System.setProperty("com.alipay.remoting.client.log.level", "error");
        }

        SpringApplication.run(HugePDServer.class);
        System.out.println("Hugegraph-pd started.");
    }
}

