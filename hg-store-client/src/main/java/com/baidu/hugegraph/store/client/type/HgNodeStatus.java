package com.baidu.hugegraph.store.client.type;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/26
 */
public enum HgNodeStatus {

    UNKNOWN(0, "UNKNOWN"),
    NOT_EXIST(100,"NOT_EXIST"),    // Failed to apply for an instance via node-id from NodeManager.
    NOT_ONLINE(105,"NOT_ONLINE"),  // Failed to connect to Store-Node at the first time.
    NOT_WORK(110,"NOT_WORK"),      // When a Store-Node to be not work anymore.

    PARTITION_COMMON_FAULT(200,"PARTITION_COMMON_FAULT"),   //
    NOT_PARTITION_LEADER(205,"NOT_PARTITION_LEADER") // When a Store-Node is not a specific partition leader.

    ;

    private int status;
    private String name;

    private HgNodeStatus(int status,String name){
        this.status=status;
        this.name=name;
    }

}
