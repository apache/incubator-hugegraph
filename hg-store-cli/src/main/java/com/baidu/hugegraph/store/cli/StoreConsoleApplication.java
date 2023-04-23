package com.baidu.hugegraph.store.cli;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.store.HgStoreClient;
import com.baidu.hugegraph.store.cli.loader.HgThread2DB;
import com.baidu.hugegraph.store.cli.scan.GrpcShardScanner;
import com.baidu.hugegraph.store.cli.scan.HgStoreCommitter;
import com.baidu.hugegraph.store.cli.scan.HgStoreScanner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;


/**
 * @author lynn.bond@hotmail.com on 2022/2/14
 */
@SpringBootApplication
@Slf4j
public class StoreConsoleApplication implements CommandLineRunner {

    @Autowired
    private AppConfig appConfig;

    public static void main(String[] args) {
        log.info("Starting StoreConsoleApplication");
        SpringApplication.run(StoreConsoleApplication.class, args);
        log.info("StoreConsoleApplication finished.");
    }

    @Override
    public void run(String... args) throws IOException, InterruptedException, PDException {
        if (args.length <= 0){
            // HgThread2DB hB = new HgThread2DB("localhost:8686");
            // hB.startMultiprocessQuery("12", "10");
            System.out.println("参数类型 cmd[-load, -query, -scan]");
        }else {
            switch (args[0]) {
                case "-load":
                    HgThread2DB hgThread2DB = new HgThread2DB(args[1]);
                    if (!args[3].isEmpty()) {
                        hgThread2DB.setGraphName(args[3]);
                    }
                    try {
                        if (args[2].equals("order")){
                            hgThread2DB.testOrder(args[4]);
                        }else {
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
                    if ( args.length < 4){
                        System.out.println("参数类型 -scan pd graphName tableName");
                    }else {
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
                    System.out.println("参数类型错误，未执行任何程序");
            }
        }
    }

    private void doCommit(){
        HgStoreCommitter committer=HgStoreCommitter.of(appConfig.getCommitterGraph());
        committer.put(appConfig.getScannerTable(),appConfig.getCommitterAmount());
    }

    private void doScan(String pd, String graphName, String tableName) throws PDException {
        HgStoreClient storeClient = HgStoreClient.create(PDConfig.of(pd)
                .setEnableCache(true));

        HgStoreScanner storeScanner = HgStoreScanner.of(storeClient, graphName);
        storeScanner.scanTable2(tableName);
      //  storeScanner.scanHash();
    }
}
