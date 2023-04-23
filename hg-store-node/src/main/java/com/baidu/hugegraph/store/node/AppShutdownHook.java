package com.baidu.hugegraph.store.node;

import com.baidu.hugegraph.rocksdb.access.RocksDBFactory;

/**
 * @author lynn.bond@hotmail.com copy from web
 */
public class AppShutdownHook extends Thread {

    private Thread mainThread;
    private boolean shutDownSignalReceived;

    public AppShutdownHook(Thread mainThread) {
        super();
        this.mainThread = mainThread;
        this.shutDownSignalReceived = false;
        Runtime.getRuntime().addShutdownHook(this);
    }

    @Override
    public void run() {
        System.out.println("Shut down signal received.");
        this.shutDownSignalReceived = true;
        mainThread.interrupt();

        doSomethingForShutdown();

        try {
            mainThread.join(); //当收到停止信号时，等待mainThread的执行完成
        } catch (InterruptedException e) {
        }
        System.out.println("Shut down complete.");
    }

    public boolean shouldShutDown() {
        return shutDownSignalReceived;
    }

    private void doSomethingForShutdown() {
       RocksDBFactory.getInstance().releaseAllGraphDB();
    }
}
