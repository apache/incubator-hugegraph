package org.apache.hugegraph.pd.boot;

import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hugegraph.pd.consts.PoolNames;
import org.apache.hugegraph.pd.service.MetadataService;

public class ShutdownHook extends Thread {

    private static Logger log = LoggerFactory.getLogger(ShutdownHook.class);
    private static String msg = "there are still uninterruptible jobs that have not been completed and" +
                                " will wait for them to complete";
    private Thread main;

    public ShutdownHook(Thread main) {
        super();
        this.main = main;
        setName(PoolNames.SHUTDOWN);
    }

    @Override
    public void run() {
        log.info("shutdown signal received");
        main.interrupt();
        shutdown();
        try {
            main.join();
        } catch (InterruptedException e) {
        }
        log.info("shutdown completed");
    }

    private void shutdown() {
        checkUninterruptibleJobs();
    }

    private void checkUninterruptibleJobs() {
        ThreadPoolExecutor jobs = MetadataService.getUninterruptibleJobs();
        try {
            if (jobs != null) {
                long lastPrint = System.currentTimeMillis() - 5000;
                log.info("check for ongoing background jobs that cannot be interrupted, active:{}, queue:{}.",
                         jobs.getActiveCount(), jobs.getQueue().size());
                while (jobs.getActiveCount() != 0 || jobs.getQueue().size() != 0) {
                    synchronized (ShutdownHook.class) {
                        if (System.currentTimeMillis() - lastPrint > 5000) {
                            log.warn(msg);
                            lastPrint = System.currentTimeMillis();
                        }
                        try {
                            ShutdownHook.class.wait(200);
                        } catch (InterruptedException e) {
                            log.error("close jobs with error:", e);
                        }
                    }
                }
                log.info("all ongoing background jobs have been completed and the shutdown will continue");
            }

        } catch (Exception e) {
            log.error("close jobs with error:", e);
        }
        try {
            if (jobs != null) {
                jobs.shutdownNow();
            }
        } catch (Exception e) {
            log.error("close jobs with error:", e);
        }
    }
}
