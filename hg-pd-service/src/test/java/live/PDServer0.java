package live;

import org.apache.hugegraph.pd.boot.HugePDServer;
import org.apache.hugegraph.pd.boot.ShutdownHook;

import org.apache.commons.io.FileUtils;
import org.springframework.boot.SpringApplication;

/**
 * for 1 store node and 1 pd
 * @author zhangyingjie
 * @date 2022/1/9
 **/
public class PDServer0 {

    static String SERVER_NAME = "server0";

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));
        SpringApplication.run(HugePDServer.class, String.format("--spring.profiles.active=%s", SERVER_NAME));
        System.out.println(SERVER_NAME + " started.");
    }

}
