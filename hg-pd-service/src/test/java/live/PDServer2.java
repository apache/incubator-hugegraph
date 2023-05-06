package live;

import org.apache.hugegraph.pd.boot.HugePDServer;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;

/**
 * @author zhangyingjie
 * @date 2022/1/9
 **/
public class PDServer2 {

    static String SERVER_NAME = "server2";
    static String DATA_PATH = "tmp/8687";

    public static void main(String[] args) {
       // deleteDirectory(new File(DATA_PATH));
        SpringApplication.run(HugePDServer.class, String.format("--spring.profiles.active=%s", SERVER_NAME));
        System.out.println(SERVER_NAME + " started.");
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.println(String.format("Failed to start ....,%s", e.getMessage()));
        }
    }

}
