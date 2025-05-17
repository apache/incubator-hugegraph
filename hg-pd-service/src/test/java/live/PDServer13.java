package live;

import org.springframework.boot.SpringApplication;

import org.apache.hugegraph.pd.boot.HugePDServer;
import org.apache.hugegraph.pd.boot.ShutdownHook;

/**
 * for one pd and three nodes
 */
public class PDServer13 {

    static String SERVER_NAME = "server13";

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));
        SpringApplication.run(HugePDServer.class, String.format("--spring.profiles.active=%s", SERVER_NAME));
        System.out.println(SERVER_NAME + " started.");
    }
}
