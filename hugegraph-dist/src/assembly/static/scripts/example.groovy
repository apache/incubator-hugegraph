import com.baidu.hugegraph.HugeFactory
import com.baidu.hugegraph.dist.RegisterUtil
import org.apache.tinkerpop.gremlin.structure.T

conf = "conf/hugegraph.properties"
graph = HugeFactory.open(conf);
schema = graph.schema();

schema.propertyKey("name").asText().ifNotExist().create();
schema.propertyKey("age").asInt().ifNotExist().create();
schema.propertyKey("city").asText().ifNotExist().create();
schema.propertyKey("weight").asDouble().ifNotExist().create();
schema.propertyKey("lang").asText().ifNotExist().create();
schema.propertyKey("date").asText().ifNotExist().create();
schema.propertyKey("price").asInt().ifNotExist().create();

schema.vertexLabel("person").properties("name", "age", "city").primaryKeys("name").ifNotExist().create();
schema.vertexLabel("software").properties("name", "lang", "price").primaryKeys("name").ifNotExist().create();
schema.indexLabel("personByName").onV("person").by("name").secondary().ifNotExist().create();
schema.indexLabel("personByCity").onV("person").by("city").secondary().ifNotExist().create();
schema.indexLabel("personByAgeAndCity").onV("person").by("age", "city").secondary().ifNotExist().create();
schema.indexLabel("softwareByPrice").onV("software").by("price").range().ifNotExist().create();
schema.edgeLabel("knows").sourceLabel("person").targetLabel("person").properties("date", "weight").ifNotExist().create();
schema.edgeLabel("created").sourceLabel("person").targetLabel("software").properties("date", "weight").ifNotExist().create();
schema.indexLabel("createdByDate").onE("created").by("date").secondary().ifNotExist().create();
schema.indexLabel("createdByWeight").onE("created").by("weight").range().ifNotExist().create();
schema.indexLabel("knowsByWeight").onE("knows").by("weight").range().ifNotExist().create();

marko = graph.addVertex(T.label, "person", "name", "marko", "age", 29, "city", "Beijing");
vadas = graph.addVertex(T.label, "person", "name", "vadas", "age", 27, "city", "Hongkong");
lop = graph.addVertex(T.label, "software", "name", "lop", "lang", "java", "price", 328);
josh = graph.addVertex(T.label, "person", "name", "josh", "age", 32, "city", "Beijing");
ripple = graph.addVertex(T.label, "software", "name", "ripple", "lang", "java", "price", 199);
peter = graph.addVertex(T.label, "person", "name", "peter", "age", 35, "city", "Shanghai");

marko.addEdge("knows", vadas, "date", "20160110", "weight", 0.5);
marko.addEdge("knows", josh, "date", "20130220", "weight", 1.0);
marko.addEdge("created", lop, "date", "20171210", "weight", 0.4);
josh.addEdge("created", lop, "date", "20091111", "weight", 0.4);
josh.addEdge("created", ripple, "date", "20171210", "weight", 1.0);
peter.addEdge("created", lop, "date", "20170324", "weight", 0.2);

graph.tx().commit();

g = graph.traversal();

System.out.println(">>>> query all vertices: size=" + g.V().toList().size());
System.out.println(">>>> query all edges: size=" + g.E().toList().size());