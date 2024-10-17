/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

@Slf4j
public class HdfsUtils implements AutoCloseable {

    private final FileSystem fileSystem;

    public HdfsUtils(String hdfsUri) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true); // 禁用 FileSystem 缓存
        fileSystem = FileSystem.get(conf);
    }

    public Map<Integer, String> parseHdfsPath(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);

        Map<Integer, String> resultMap = new ConcurrentHashMap<>();
        FileStatus[] fileStatuses = fileSystem.listStatus(path);

        // 正则表达式匹配文件名中的特定部分
        String regex = ".*(\\d{5})\\.sst";
        Pattern pattern = Pattern.compile(regex);

        // 使用并行流处理文件
        Arrays.stream(fileStatuses).parallel().forEach(fileStatus -> {
            if (fileStatus.isFile()) {
                String filePath = fileStatus.getPath().toString();
                Matcher matcher = pattern.matcher(filePath);
                if (matcher.find()) {
                    String key = matcher.group(1);
                    resultMap.put(Integer.parseInt(key), filePath);
                }
            }
        });

        return resultMap;
    }

    public String downloadFile(String hdfsPath, String localPath, int rateLimitKbps) throws IOException {
        Path path = new Path(hdfsPath);
        if (!fileSystem.exists(path)) {
            throw new IOException("File does not exist: " + hdfsPath);
        }
        if (fileSystem.isDirectory(path)) {
            throw new IOException("Path is a directory: " + hdfsPath);
        }

        // 提取文件名并拼接到localPath上
        String fileName = path.getName();
        File localFile = new File(localPath, fileName);
        if (!localFile.getParentFile().exists()) {
            localFile.getParentFile().mkdirs();
        }

        // 获取文件总大小
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        long totalSize = fileStatus.getLen();

        int maxRetries = 3;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (FSDataInputStream in = fileSystem.open(path);
                 FileOutputStream out = new FileOutputStream(localFile)) {
                copyBytesWithRateLimit(in, out, 4096, rateLimitKbps, totalSize);
                return localFile.getAbsolutePath();
            } catch (IOException e) {
                log.error("Download failed, retrying... (Attempt {}/{})", retry + 1, maxRetries, e);
                // 清空之前的下载
                if (localFile.exists()) {
                    localFile.delete();
                }
            }
        }
        throw new IOException("Failed to download file after " + maxRetries + " attempts.");
    }

    public void downloadDirectory(String hdfsPath, String localPath, int rateLimitKbps) throws IOException {
        Path path = new Path(hdfsPath);
        if (!fileSystem.exists(path)) {
            throw new IOException("Directory does not exist: " + hdfsPath);
        }
        if (!fileSystem.isDirectory(path)) {
            throw new IOException("Path is not a directory: " + hdfsPath);
        }
        File localDir = new File(localPath);
        if (!localDir.exists()) {
            localDir.mkdirs();
        }

        int maxRetries = 3;
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                RemoteIterator<LocatedFileStatus>
                        fileStatusListIterator = fileSystem.listFiles(path, true);
                while (fileStatusListIterator.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatusListIterator.next();
                    Path filePath = fileStatus.getPath();
                    String relativePath = filePath.toString().substring(hdfsPath.length());
                    File localFile = new File(localPath + File.separator + relativePath);
                    if (!localFile.getParentFile().exists()) {
                        localFile.getParentFile().mkdirs();
                    }
                    long totalSize = fileStatus.getLen();
                    try (FSDataInputStream in = fileSystem.open(filePath);
                         FileOutputStream out = new FileOutputStream(localFile)) {
                        copyBytesWithRateLimit(in, out, 4096, rateLimitKbps, totalSize);
                    }
                }
                return;
            } catch (IOException e) {
                log.error("Download failed, retrying... (Attempt {}/{})", retry + 1, maxRetries, e);
                // 清空之前的下载
                if (localDir.exists()) {
                    deleteDirectory(localDir);
                }
            }
        }
        throw new IOException("Failed to download directory after " + maxRetries + " attempts.");
    }

    private void deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }

    private void copyBytesWithRateLimit(FSDataInputStream in, FileOutputStream out, int bufferSize, int rateLimitKbps, long totalSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int bytesRead;
        long startTime = System.currentTimeMillis();
        long bytesReadInSecond = 0;
        long totalBytesRead = 0;

        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
            bytesReadInSecond += bytesRead;
            totalBytesRead += bytesRead;

            // 计算并显示进度
            double progress = (double) totalBytesRead / totalSize * 100;
            log.info("Download progress: %.2f%%%n", progress);
            // 如果读取的字节数超过了限速值，进行延迟
            if (bytesReadInSecond >= rateLimitKbps * 1024) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime < 1000) {
                    try {
                        Thread.sleep(1000 - elapsedTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Thread interrupted during rate limiting", e);
                    }
                }
                startTime = System.currentTimeMillis();
                bytesReadInSecond = 0;
            }
        }
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
    }

    public static HDFSUriPath extractHdfsUriAndPath(String hdfsPath) {
        // 正则表达式匹配HDFS URI和路径
        String regex = "^(hdfs://[^/]+)(/.*)?$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(hdfsPath);

        if (matcher.matches()) {
            String hdfsUri = matcher.group(1);
            String hdfsFilePath = matcher.group(2);
            validateHdfsUri(hdfsUri);
            return new HDFSUriPath(hdfsUri, hdfsFilePath);
        } else {
            throw new IllegalArgumentException("The provided HDFS path is not valid.");
        }
    }

    public static void validateHdfsUri(String hdfsUri) {
        try {
            URI uri = new URI(hdfsUri);
            if (!"hdfs".equals(uri.getScheme())) {
                throw new IllegalArgumentException("The URI scheme is not 'hdfs'.");
            }
            if (uri.getHost() == null || uri.getPort() == -1) {
                throw new IllegalArgumentException("The URI must include a host and port.");
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The URI syntax is not valid: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // 正则表达式匹配文件名中的特定部分
        String regex = ".*r-(\\d+)\\.sst";
        Pattern pattern = Pattern.compile(regex);
        String filePath = "hdfs://txy-hn1-bigdata-hdp11-nn-prd-02.myhll.cn:8020/user/hdfs/hg-1_5/gen-sstfile/1726305291103/part-r-0000.sst";
        Matcher matcher = pattern.matcher(filePath);
        if (matcher.find()) {
            String key = matcher.group(1);
            System.out.println("Key: " + key);
            int i = Integer.parseInt(key);

            System.out.println("Key: " + i);
        }
    }

    public static class HDFSUriPath {
        private final String hdfsUri;
        private final String hdfsPath;

        public HDFSUriPath(String hdfsUri, String hdfsPath) {
            this.hdfsUri = hdfsUri;
            this.hdfsPath = hdfsPath;
        }

        public String getHdfsUri() {
            return hdfsUri;
        }

        public String getHdfsPath() {
            return hdfsPath;
        }
    }
}
