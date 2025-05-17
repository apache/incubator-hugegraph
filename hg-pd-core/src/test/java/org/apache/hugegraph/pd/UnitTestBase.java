package org.apache.hugegraph.pd;

import java.io.File;

public class UnitTestBase {
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
