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

package org.apache.hugegraph.store.cli;

import java.io.IOException;

import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.cli.loader.HgThread2DB;
import org.apache.hugegraph.store.cli.scan.GrpcShardScanner;
import org.apache.hugegraph.store.cli.scan.HgStoreScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

/**
 * 2022/2/14
 */
@SpringBootApplication
@Slf4j
public class StoreConsoleApplication implements CommandLineRunner {

    // TODO: this package seems to have many useless class and code, need to be updated.
    @Autowired
    private AppConfig appConfig;

    public static void main(String[] args) {
        log.info("Starting StoreConsoleApplication");
        SpringApplication.run(StoreConsoleApplication.class, args);
        log.info("StoreConsoleApplication finished.");
    }

    @Override
    public void run(String... args) throws IOException, InterruptedException, PDException {
        if (args.length <= 0) {
            log.warn("参数类型 cmd[-load, -query, -scan]");
        } else {
            switch (args[0]) {
                case "-load":
                    HgThread2DB hgThread2DB = new HgThread2DB(args[1]);
                    if (!args[3].isEmpty()) {
                        hgThread2DB.setGraphName(args[3]);
                    }
                    try {
                        if ("order".equals(args[2])) {
                            hgThread2DB.testOrder(args[4]);
                        } else {
                            hgThread2DB.startMultiprocessInsert(args[2]);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "-query":
                    HgThread2DB hgDB = new HgThread2DB(args[1]);
                    try {
                        hgDB.startMultiprocessQuery("12", args[2]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case "-scan":
                    if (args.length < 4) {
                        log.warn("参数类型 -scan pd graphName tableName");
                    } else {
                        doScan(args[1], args[2], args[3]);
                    }
                    break;
                case "-shard":
                    GrpcShardScanner scanner = new GrpcShardScanner();
                    scanner.getData();
                    break;
                case "-shard-single":
                    scanner = new GrpcShardScanner();
                    scanner.getDataSingle();
                    break;
                default:
                    log.warn("参数类型错误，未执行任何程序");
            }
        }
    }

    private void doScan(String pd, String graphName, String tableName) throws PDException {
        HgStoreClient storeClient = HgStoreClient.create(PDConfig.of(pd)
                                                                 .setEnableCache(true));

        HgStoreScanner storeScanner = HgStoreScanner.of(storeClient, graphName);
        storeScanner.scanTable2(tableName);
    }
}
