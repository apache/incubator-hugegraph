package rocksdb;

import com.baidu.hugegraph.rocksdb.access.RocksDBFactory;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.rocksdb.access.SessionOperator;
import org.junit.Test;

public class RocksDBFactoryTest extends BaseRocksDbTest{
    @Test
    public void testCreateSession() {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        try (RocksDBSession dbSession = factory.createGraphDB("./tmp", "test1")) {
            SessionOperator op = dbSession.sessionOp();
            op.prepare();
            try {
                op.put("tbl", "k1".getBytes(), "v1".getBytes());
                op.commit();
            }catch (Exception e){
                op.rollback();
            }

        }
        factory.destroyGraphDB("test1");
    }
     @Test
    public void testTotalKeys(){
        RocksDBFactory dbFactory = RocksDBFactory.getInstance();
        System.out.println(dbFactory.getTotalSize());

        System.out.println( dbFactory.getTotalKey().entrySet()
                .stream().map(e->e.getValue()).reduce(0L, Long::sum));
    }
     @Test
    public void releaseAllGraphDB() {
        System.out.println(RocksDBFactory.class);

        RocksDBFactory rFactory  = RocksDBFactory.getInstance();

        if (rFactory.queryGraphDB("bj01") == null) {
            rFactory.createGraphDB("./tmp", "bj01");
        }

        if (rFactory.queryGraphDB("bj02") == null) {
            rFactory.createGraphDB("./tmp", "bj02");
        }

        if (rFactory.queryGraphDB("bj03") == null) {
            rFactory.createGraphDB("./tmp", "bj03");
        }

        RocksDBSession dbSession = rFactory.queryGraphDB("bj01");

        dbSession.checkTable("test");
        SessionOperator sessionOp  = dbSession.sessionOp();
        sessionOp.prepare();

        sessionOp.put("test", "hi".getBytes(), "byebye".getBytes());
        sessionOp.commit();

        rFactory.releaseAllGraphDB();
    }
}
