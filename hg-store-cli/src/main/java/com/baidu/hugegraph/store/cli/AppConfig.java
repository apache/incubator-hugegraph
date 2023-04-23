package com.baidu.hugegraph.store.cli;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author lynn.bond@hotmail.com on 2022/2/15
 */
@Data
@Component
public class AppConfig {

    @Value("${pd.address}")
    private String pdAddress;

    @Value("${net.kv.scanner.page.size}")
    private int scannerPageSize;

    @Value("${scanner.graph}")
    private String scannerGraph;

    @Value("${scanner.table}")
    private String scannerTable;

    @Value("${scanner.max}")
    private int scannerMax;

    @Value("${scanner.mod}")
    private int scannerModNumber;

    @Value("${committer.graph}")
    private String committerGraph;

    @Value("${committer.table}")
    private String committerTable;

    @Value("${committer.amount}")
    private int committerAmount;
}
