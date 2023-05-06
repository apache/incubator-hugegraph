package org.apache.hugegraph.pd.core;

import org.apache.hugegraph.pd.core.meta.MetadataKeyHelperTest;

import lombok.extern.slf4j.Slf4j;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
        StoreNodeServiceTest.class,
        MetadataKeyHelperTest.class
})

@Slf4j
public class PDCoreSuiteTest {


}