package com.baidu.hugegraph.example;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by jishilei on 2017/4/2.
 */
public class Example2 {
    private static final Logger logger = LoggerFactory.getLogger(Example2.class);

    public static void main(String[] args) {
        logger.info("Example2 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example2.load(graph);
        showSchema(graph);
        traversal(graph);

        System.exit(0);
    }

    public static void traversal(final HugeGraph graph) {

        GraphTraversal<Vertex, Vertex> vertexs = graph.traversal().V();
        System.out.println(">>>> query all vertices: size="
                + vertexs.toList().size());

        GraphTraversal<Edge, Edge> edges = graph.traversal().E();
        System.out.println(">>>> query all edges: size="
                + edges.toList().size());

        List<Object> names = graph.traversal().V().inE(
                "knows").limit(2).outV().values("name").toList();
        System.out.println(">>>> query vertex(with props) of edges: "
                + names);
        assert names.size() == 2 : names.size();

        names = graph.traversal().V().as("a")
                .out("knows")
                .and()
                .out("created").in("created").as("a").values("name")
                .toList();
        System.out.println(">>>> query with AND: " + names);
        assert names.size() == 1 : names.size();

        List<Path> paths = graph.traversal().V("person\u0002marko")
                .out().out().path().by("name").toList();
        System.out.println(">>>> test out path: " + paths);
        assert paths.size() == 2;
        assert paths.get(0).get(0).equals("marko");
        assert paths.get(0).get(1).equals("josh");
        assert paths.get(0).get(2).equals("lop");
        assert paths.get(1).get(0).equals("marko");
        assert paths.get(1).get(1).equals("josh");
        assert paths.get(1).get(2).equals("ripple");

        paths = shortestPath(graph, "person\u0002marko", "software\u0002lop", 5);
        System.out.println(">>>> test shortest path: " + paths.get(0));
        assert paths.get(0).get(0).equals("marko");
        assert paths.get(0).get(1).equals("lop");
    }

    public static List<Path> shortestPath(final HugeGraph graph,
            Object from, Object to, int maxDepth) {
        GraphTraversal<Vertex, Path> t = graph.traversal()
                .V(from)
                .repeat(__.out().simplePath())
                .until(__.hasId(to).or().loops().is(P.gt(maxDepth)))
                .hasId(to)
                .path().by("name")
                .limit(1);
        return t.toList();
    }

    public static void showSchema(final HugeGraph graph) {
        SchemaManager schemaManager = graph.schema();

        logger.info("===============  show schema  ================");

        List<SchemaElement> elements = schemaManager.desc();
        for (SchemaElement element : elements) {
            System.out.println(element.schema());
        }
    }

    public static void load(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        /**
         * Note:
         * Use schema.makePropertyKey interface to create propertyKey.
         * Use schema.propertyKey interface to query propertyKey.
         */
        schema.makePropertyKey("name").asText().ifNotExist().create();
        schema.makePropertyKey("age").asInt().ifNotExist().create();
        schema.makePropertyKey("lang").asText().ifNotExist().create();
        schema.makePropertyKey("date").asText().ifNotExist().create();
        schema.makePropertyKey("price").asInt().ifNotExist().create();

        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.makeVertexLabel("person").properties("date").append();
//        person.properties("date").append();

        VertexLabel software = schema.makeVertexLabel("software")
                .properties("name", "lang", "price")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.makeIndexLabel("personByName").on(person).by("name").secondary()
                .ifNotExist().create();

        schema.makeIndexLabel("softwareByPrice").on(software).by("price").search()
                .ifNotExist().create();
        //        schema.makeIndexLabel("softwareByLang").on(software).by("lang")
        // .search().ifNotExist().create();

        schema.makeEdgeLabel("knows").link("person", "person")
                .properties("date").create();

        schema.makeEdgeLabel("knows").link("software", "software")
                .properties("price").append();

        EdgeLabel created = schema.makeEdgeLabel("created")
                .link("person", "software")
                .properties("date")
                .ifNotExist()
                .create();

        schema.makeIndexLabel("createdByDate").on(created).by("date").secondary()
                .ifNotExist()
                .create();

        schema.desc();

        graph.tx().open();

        Vertex marko = graph.addVertex(T.label, "person",
                "name", "marko", "age", 29);
        Vertex vadas = graph.addVertex(T.label, "person",
                        "name", "vadas", "age", 27);
        Vertex lop = graph.addVertex(T.label, "software",
                "name", "lop", "lang", "java", "price", 328);
        Vertex josh = graph.addVertex(T.label, "person",
                "name", "josh", "age", 32);
        Vertex ripple = graph.addVertex(T.label, "software",
                "name", "ripple", "lang", "java", "price", 199);
        Vertex peter = graph.addVertex(T.label, "person",
                "name", "peter", "age", 35);

        marko.addEdge("knows", vadas, "date", "20160110");
        marko.addEdge("knows", josh, "date", "20130220");
        marko.addEdge("created", lop, "date", "20171210");
        josh.addEdge("created", ripple, "date", "20171210");
        josh.addEdge("created", lop, "date", "20091111");
        peter.addEdge("created", lop, "date", "20170324");

        graph.tx().commit();
    }
}
