package com.baidu.hugegraph.store.boot;

import com.baidu.hugegraph.store.node.StoreNodeApplication;
import org.springframework.boot.SpringApplication;

import java.io.File;

public class StoreNodeServer06 {

    public static void main(String[] args) {
       // deleteDir(new File("tmp/8503"));
        SpringApplication.run(StoreNodeApplication.class,"--spring.profiles.active=server06");
        System.out.println("StoreNodeServer03 started.");
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