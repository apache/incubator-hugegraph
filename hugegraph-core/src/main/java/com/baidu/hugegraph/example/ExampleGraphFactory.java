package com.baidu.hugegraph.example;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;

/**
 * Created by jishilei on 17/3/16.
 */
public class ExampleGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(ExampleGraphFactory.class);

    public static void main(String[] args) {

        logger.info("ExampleGraphFactory start!");

        String confFile = ExampleGraphFactory.class.getClassLoader().getResource("hugegraph.properties").getPath();
        HugeGraph graph = HugeFactory.open(confFile);
        graph.clearBackend();
        graph.initBackend();

        ExampleGraphFactory.showFeatures(graph);
        ExampleGraphFactory.load(graph);
    }

    public static void showFeatures(final HugeGraph graph) {
        logger.info("supportsPersistence : " + graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

        /************************* schemaManager operating *************************/
        SchemaManager schema = graph.schema();
        logger.info("===============  propertyKey  ================");
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("timestamp").asTimestamp().create();
        schema.propertyKey("ISBN").asText().create();
        schema.propertyKey("calories").asInt().create();
        schema.propertyKey("amount").asText().create();
        schema.propertyKey("stars").asInt().create();
        schema.propertyKey("age").asText().valueSingle().create();
        schema.propertyKey("comment").asText().valueSet().create();
        schema.propertyKey("nickname").asText().valueList().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("country").asText().valueSet().properties("livedIn").create();
        schema.propertyKey("city_id").asInt().create();
        schema.propertyKey("sensor_id").asUuid().create();

        logger.info("===============  vertexLabel  ================");

        schema.vertexLabel("person").properties("name", "age", "city").primaryKeys("name").create();
        schema.vertexLabel("author").properties("id", "name").primaryKeys("id").create();
        schema.vertexLabel("language").properties("name").primaryKeys("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions").create();
        schema.vertexLabel("ingredient").create();
        schema.vertexLabel("book").properties("name").primaryKeys("name").create();
        schema.vertexLabel("meal").create();
        schema.vertexLabel("reviewer").create();
        // vertex label must have the properties that specified in primary key
        schema.vertexLabel("FridgeSensor").properties("city_id").primaryKeys("city_id").create();

        logger.info("===============  vertexLabel & index  ================");
        // TODO: implement index feature.
        schema.vertexLabel("person").index("personByCity").secondary().by("city").create();
        schema.vertexLabel("person").index("personByAge").search().by("age").create();
        // schemaManager.vertexLabel("author").index("byName").secondary().by("name").add();
        // schemaManager.vertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        // schemaManager.vertexLabel("meal").index("byMeal").materialized().by("name").add();
        // schemaManager.vertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        // schemaManager.vertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        logger.info("===============  edgeLabel  ================");

        schema.edgeLabel("authored").singleTime().linkOne2One().properties("contribution").create();
        schema.edgeLabel("created").singleTime().linkMany2Many().create();
        schema.edgeLabel("includes").singleTime().linkOne2Many().create();
        schema.edgeLabel("includedIn").linkMany2One().create();
        schema.edgeLabel("rated").linkMany2Many().link("reviewer", "recipe").create();


        logger.info("===============  schemaManager desc  ================");
        //schemaManager.desc();

        /************************* data operating *************************/

        // Directly into the back-end
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Tom",
                "city", "Beijing", "age", "18");
        graph.addVertex(T.label, "person", "name", "James",
                "city", "Beijing", "age", "19");
        graph.addVertex(T.label, "person", "name", "Cat",
                "city", "Beijing", "age", "20");
        graph.addVertex(T.label, "person", "name", "Lisa",
                "city", "Beijing", "age", "20");
        graph.addVertex(T.label, "person", "name", "Hebe",
                "city", "Taipei", "age", "21");

        // Must commit manually
        GraphTransaction tx = graph.openTransaction();

        logger.info("===============  addVertex  ================");
        Vertex person = tx.addVertex(T.label, "author",
                "id", 1, "name", "James Gosling", "age", "60", "lived", "");

        Vertex java = tx.addVertex(T.label, "language", "name", "java");
        Vertex book1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex book2 = tx.addVertex(T.label, "book", "name", "java-2");

        person.addEdge("created", java);
        person.addEdge("authored", book1,
                "contribution", "1990-1-1",
                "comment", "it's a good book",
                "comment", "it's a good book",
                "comment", "it's a good book too");
        person.addEdge("authored", book2, "contribution", "2017-4-28");

        // commit data changes
        try {
            tx.commit();
        } catch (BackendException e) {
            e.printStackTrace();
            try {
                tx.rollback();
            } catch (BackendException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
        }

        // use the default Transaction to commit
        graph.addVertex(T.label, "book", "name", "java-3");

        // tinkerpop tx
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");
        graph.tx().commit();

        // query all
        GraphTraversal<Vertex, Vertex> vertex = graph.traversal().V();
        System.out.println(">>>> query all vertices: size=" + vertex.toList().size());

        // query vertex by id
        vertex = graph.traversal().V("author\u00021");
        GraphTraversal<Vertex, Edge> edgesOfVertex = vertex.outE("created");
        System.out.println(">>>> query edges of vertex: " + edgesOfVertex.toList());

        vertex = graph.traversal().V("author\u00021");
        GraphTraversal<Vertex, Vertex> verticesOfVertex = vertex.out("created");
        System.out.println(">>>> query vertices of vertex: " + verticesOfVertex.toList());

        // query edge by condition
        ConditionQuery q = new ConditionQuery(HugeTypes.VERTEX);
        q.query(IdGeneratorFactory.generator().generate("author\u00021"));
        q.eq(HugeKeys.PROPERTY_KEY, "age");

        Iterator<Vertex> vertices = graph.vertices(q);
        System.out.println(">>>> queryVertices(): " + vertices.hasNext());
        while (vertices.hasNext()) {
            System.out.println(">>>> " + vertices.next().toString());
        }

        // query all edges
        GraphTraversal<Edge, Edge> edges = graph.traversal().E().limit(2);
        System.out.println(">>>> query all edges: size=" + edges.toList().size());

        // query edge by id
        String id = "author\u00021\u0001OUT\u0001authored\u0001\u0001book\u0002java-2";
        edges = graph.traversal().E(id);
        System.out.println(">>>> query edge by id: " + edges.toList());

        // query edge by condition
        q = new ConditionQuery(HugeTypes.EDGE);
        q.eq(HugeKeys.SOURCE_VERTEX, "author\u00021");
        q.eq(HugeKeys.DIRECTION, Direction.OUT.name());
        q.eq(HugeKeys.LABEL, "authored");
        q.eq(HugeKeys.SORT_VALUES, "");
        q.eq(HugeKeys.TARGET_VERTEX, "book\u0002java-1");
        q.eq(HugeKeys.PROPERTY_KEY, "contribution");

        Iterator<Edge> edges2 = graph.edges(q);
        System.out.println(">>>> queryEdges(): " + edges2.hasNext());
        while (edges2.hasNext()) {
            System.out.println(">>>> " + edges2.next().toString());
        }

        // query by vertex label
        vertex = graph.traversal().V().hasLabel("book");
        System.out.println(">>>> query all books: size=" + vertex.toList().size());

        // query by vertex props
        vertex = graph.traversal().V().hasLabel("person").has("city", "Taipei");
        System.out.println(">>>> query all persons in Taipei: " + vertex.toList());

        vertex = graph.traversal().V().hasLabel("person").has("age", "19");
        System.out.println(">>>> query all persons age==19: " + vertex.toList());

        vertex = graph.traversal().V().hasLabel("person").has("age", P.lt("19"));
        System.out.println(">>>> query all persons age<19: " + vertex.toList());
    }

}
