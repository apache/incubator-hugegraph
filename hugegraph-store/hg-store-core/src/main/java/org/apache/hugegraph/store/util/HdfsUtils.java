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

package org.apache.hugegraph.store.util;

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
    int lastLoggedProgress = 0;

    public HdfsUtils(String hdfsUri) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.setBoolean("fs.hdfs.impl.disable.cache", true); // disable FileSystem cache
        fileSystem = FileSystem.get(conf);
    }




    public Map<String, String> parseHdfsPath(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);

        Map<String, String> resultMap = new ConcurrentHashMap<>();
        FileStatus[] fileStatuses = fileSystem.listStatus(path);

        String regex = ".*(\\d{5})\\.sst";
        Pattern pattern = Pattern.compile(regex);

        Arrays.stream(fileStatuses).parallel().forEach(fileStatus -> {
            if (fileStatus.isFile()) {
                String filePath = fileStatus.getPath().toString();
                Matcher matcher = pattern.matcher(filePath);
                if (matcher.find()) {
                    String key = matcher.group(1);
                    resultMap.put(key, filePath);
                }
            }
        });

        return resultMap;
    }

    public  String  downloadFile(String hdfsPath, String localPath, int rateLimitKbps) throws IOException {
        Path path = new Path(hdfsPath);
        if (!fileSystem.exists(path)) {
            throw new IOException("File does not exist: " + hdfsPath);
        }
        if (fileSystem.isDirectory(path)) {
            throw new IOException("Path is a directory: " + hdfsPath);
        }

        String fileName = path.getName();
        File localFile = new File(localPath, fileName);
        if(localFile.exists()){
            localFile.delete();
        }
        if (!localFile.getParentFile().exists()) {
            localFile.getParentFile().mkdirs();
        }

        FileStatus fileStatus = fileSystem.getFileStatus(path);
        long totalSize = fileStatus.getLen();
        log.info("start Downloading file: {}, size: {} bytes", fileName, totalSize);
        int maxRetries = 3;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (FSDataInputStream in = fileSystem.open(path);
                 FileOutputStream out = new FileOutputStream(localFile)) {
                copyBytesWithRateLimit(in, out, 4096, rateLimitKbps,totalSize,fileName);
                return localFile.getAbsolutePath();
            } catch (IOException e) {
                log.error("Download {} failed, retrying... (Attempt {}/{})",hdfsPath, retry + 1, maxRetries, e);
                if (localFile.exists()) {
                    localFile.delete();
                }
            }
        }
        throw new IOException("Failed to download "+hdfsPath+" file after " + maxRetries + " attempts.");


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
                        copyBytesWithRateLimit(in, out, 4096, rateLimitKbps,totalSize,filePath.getName());
                    }
                }
                return;
            } catch (IOException e) {
                log.error("Download failed, retrying... (Attempt {}/{})", retry + 1, maxRetries, e);
                if (localDir.exists()) {
                    deleteDirectory(localDir);
                }
                throw new IOException("Failed to download directory after " + maxRetries + " attempts.");
            }
        }

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

    private void copyBytesWithRateLimit(FSDataInputStream in, FileOutputStream out, int bufferSize, int rateLimitKbps,long totalSize,String fileName) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int bytesRead;
        long startTime = System.currentTimeMillis();
        long bytesReadInSecond = 0;
        long totalBytesRead = 0;


        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
            bytesReadInSecond += bytesRead;
            totalBytesRead += bytesRead;

            double progress = (double) totalBytesRead / totalSize * 100;
            int currentProgress = (int) progress;

            if (currentProgress / 10 > lastLoggedProgress / 10) {
                log.info("fileName: {} , Download progress: {}%", fileName, String.format("%.2f", progress));
                lastLoggedProgress = currentProgress;
            }
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
