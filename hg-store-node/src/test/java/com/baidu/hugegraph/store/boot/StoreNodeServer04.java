package com.baidu.hugegraph.store.boot;

import com.baidu.hugegraph.store.node.StoreNodeApplication;
import org.springframework.boot.SpringApplication;

import java.io.File;

public class StoreNodeServer04 {

    public static void main(String[] args) {
        // deleteDir(new File("tmp/8504"));
        SpringApplication.run(StoreNodeApplication.class,"--spring.profiles.active=server04");
        System.out.println("StoreNodeServer04 started.");
    }
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for(File file : dir.listFiles()){
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}