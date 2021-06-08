package com.baidu.hugegraph.api.traversers;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.dist.RegisterUtil;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    AllShortestPathsAPITest.class,
    CountAPITest.class,
    CrosspointsAPITest.class,
    CustomizedCrosspointsAPITest.class,
    EdgesAPITest.class,
    FusiformSimilarityAPITest.class,
    JaccardSimilarityAPITest.class,
    KneighborAPITest.class,
    KoutAPITest.class,
    MultiNodeShortestPathAPITest.class,
    NeighborRankAPITest.class,
    PathsAPITest.class,
    PersonalRankAPITest.class,
    RaysAPITest.class,
    RingsAPITest.class,
    SameNeighborsAPITest.class,
    ShortestPathAPITest.class,
    SingleSourceShortestPathAPITest.class,
    TemplatePathsAPITest.class
})
public class TraversersApiTestSuite {
}
