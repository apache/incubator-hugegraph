package org.apache.hugegraph.pd.watch;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
enum WatchType {

    PARTITION_CHANGE(10);

    private int value;

    private WatchType(int value){
        this.value=value;
    }

}
