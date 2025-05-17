package live;

import org.springframework.boot.SpringApplication;

import org.apache.hugegraph.pd.boot.HugePDServer;
import org.apache.hugegraph.pd.boot.ShutdownHook;

/** for 3 store nodes and 3 pds
 * @author zhangyingjie
 * @date 2022/1/9
 **/
public class PDServer1 {

    static String SERVER_NAME = "server1";

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(Thread.currentThread()));
        SpringApplication.run(HugePDServer.class, String.format("--spring.profiles.active=%s", SERVER_NAME));
        System.out.println(SERVER_NAME + " started.");
    }

}
