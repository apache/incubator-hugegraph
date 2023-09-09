import org.apache.hugegraph.HugeFactory
import org.apache.hugegraph.dist.RegisterUtil

// register all the backend to avoid changes if docker needs to support othre backend
RegisterUtil.registerPlugins()
RegisterUtil.registerRocksDB()
RegisterUtil.registerCassandra()
RegisterUtil.registerScyllaDB()
RegisterUtil.registerHBase()
RegisterUtil.registerMysql()
RegisterUtil.registerPalo()
RegisterUtil.registerPostgresql()

graph = HugeFactory.open('./conf/graphs/hugegraph.properties')
