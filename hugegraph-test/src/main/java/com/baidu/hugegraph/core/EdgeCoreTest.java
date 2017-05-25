package com.baidu.hugegraph.core;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.core.FakeObjects.FakeEdge;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.common.collect.ImmutableSet;

public class EdgeCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        logger.info("===============  propertyKey  ================");

        schema.makePropertyKey("id").asInt().create();
        schema.makePropertyKey("name").asText().create();
        schema.makePropertyKey("dynamic").asBoolean().create();
        schema.makePropertyKey("time").asText().create();
        schema.makePropertyKey("timestamp").asLong().create();
        schema.makePropertyKey("age").asInt().valueSingle().create();
        schema.makePropertyKey("comment").asText().valueSet().create();
        schema.makePropertyKey("contribution").asText().create();
        schema.makePropertyKey("score").asInt().create();
        schema.makePropertyKey("lived").asText().create();
        schema.makePropertyKey("city").asText().create();

        logger.info("===============  vertexLabel  ================");

        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author")
                .properties("id", "name")
                .primaryKeys("id")
                .create();
        schema.makeVertexLabel("language")
                .properties("name", "dynamic")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("book")
                .properties("name")
                .primaryKeys("name")
                .create();

        logger.info("===============  vertexLabel index  ================");

        schema.makeIndex("personByCity").on(person).secondary()
                .by("city").create();
        schema.makeIndex("personByAge").on(person).search()
                .by("age").create();

        logger.info("===============  edgeLabel  ================");

        EdgeLabel transfer = schema.makeEdgeLabel("transfer")
                .properties("id", "timestamp")
                .multiTimes().sortKeys("id")
                .link("person", "person")
                .create();
        EdgeLabel authored = schema.makeEdgeLabel("authored").singleTime()
                .properties("contribution", "comment", "score")
                .link("author", "book")
                .create();
        schema.makeEdgeLabel("look").properties("time")
                .multiTimes().sortKeys("time")
                .link("author", "book")
                .link("person", "book")
                .create();
        schema.makeEdgeLabel("created").singleTime()
                .link("author", "language")
                .create();

        logger.info("===============  edgeLabel index  ================");

        schema.makeIndex("transferByTimestamp").on(transfer).search()
                .by("timestamp").create();

        // schema.makeIndex("authoredByScore").on(authored).secondary()
        //        .by("score").create();
    }

    @Test
    public void testAddEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                "name", "Guido van Rossum", "age", 61, "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3);
    }

    @Test
    public void testAddEdgeWithOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");
        Vertex guido =  graph.addVertex(T.label, "author", "id", 2,
                "name", "Guido van Rossum", "age", 61, "lived", "California");

        Vertex java = graph.addVertex(T.label, "language", "name", "java");
        Vertex python = graph.addVertex(T.label, "language", "name", "python",
                "dynamic", true);

        Vertex java1 = graph.addVertex(T.label, "book", "name", "java-1");
        Vertex java2 = graph.addVertex(T.label, "book", "name", "java-2");
        Vertex java3 = graph.addVertex(T.label, "book", "name", "java-3");

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("created", java);
        guido.addEdge("created", python);

        james.addEdge("authored", java1);
        james.addEdge("authored", java2);
        james.addEdge("authored", java3, "score", 4);

        james.addEdge("authored", java1);
        james.addEdge("authored", java3, "score", 5);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(5, edges.size());
        assertContains(edges, "created", james, java);
        assertContains(edges, "created", guido, python);
        assertContains(edges, "authored", james, java1);
        assertContains(edges, "authored", james, java2);
        assertContains(edges, "authored", james, java3, "score", 5);
    }

    @Test
    public void testAddEdgeWithProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("look", book, "time", "2017-4-28");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "look", james, book,
                "time", "2017-4-28");
    }

    @Test
    public void testAddEdgeWithProps() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                "contribution", "1990-1-1",
                "score", 5);

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book,
                "contribution", "1990-1-1",
                "score", 5);
    }

    @Test
    public void testAddEdgeWithPropSet() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                "comment", "good book!",
                "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(
                ImmutableSet.of("good book!", "good book too!"),
                comments);
    }

    @Test
    public void testAddEdgeWithPropSetAndOverridProp() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book,
                "comment", "good book!",
                "comment", "good book!",
                "comment", "good book too!");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(1, edges.size());
        assertContains(edges, "authored", james, book);
        Edge edge = edges.get(0);
        Object comments = edge.property("comment").value();
        Assert.assertEquals(
                ImmutableSet.of("good book!", "good book too!"),
                comments);
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimes() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("look", book, "time", "2017-4-28");
        james.addEdge("look", book, "time", "2017-5-21");
        james.addEdge("look", book, "time", "2017-5-25");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "look", james, book,
                "time", "2017-4-28");
        assertContains(edges, "look", james, book,
                "time", "2017-5-21");
        assertContains(edges, "look", james, book,
                "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeToSameVerticesWithMultiTimesAndOverrideEdge() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("look", book, "time", "2017-4-28");
        james.addEdge("look", book, "time", "2017-5-21");
        james.addEdge("look", book, "time", "2017-5-25");

        james.addEdge("look", book, "time", "2017-4-28");
        james.addEdge("look", book, "time", "2017-5-21");

        List<Edge> edges = graph.traversal().E().toList();
        Assert.assertEquals(3, edges.size());
        assertContains(edges, "look", james, book,
                "time", "2017-4-28");
        assertContains(edges, "look", james, book,
                "time", "2017-5-21");
        assertContains(edges, "look", james, book,
                "time", "2017-5-25");
    }

    @Test
    public void testAddEdgeWithNotExistsEdgeLabel() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("label-not-exists", book, "time", "2017-4-28");
        });
    }

    @Test
    public void testAddEdgeWithoutSortValues() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("look", book);
        });
    }

    @Test
    public void testAddEdgeWithNotExistsPropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "propkey-not-exists", "value");
        });
    }

    @Test
    public void testAddEdgeWithNotExistsEdgePropKey() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "age", 18);
        });
    }

    @Test
    public void testAddEdgeWithInvalidPropValueType() {
        HugeGraph graph = graph();

        Vertex james = graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");

        Vertex book = graph.addVertex(T.label, "book", "name", "Test-Book-1");

        james.addEdge("authored", book, "score", 5);

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", 5.1);
        });
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "five");
        });
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            james.addEdge("authored", book, "score", "5");
        });
    }

    private Vertex vertex(String label, String pkName, String pkValue) {
        List<Vertex> vertexes = graph().traversal().V().hasLabel(
                label).has(pkName, pkValue).toList();
        Assert.assertEquals(1, vertexes.size());
        return vertexes.get(0);
    }

    private static void assertContains(
            List<Edge> edges,
            String label,
            Vertex outVertex,
            Vertex inVertex,
            Object... kvs) {
        Assert.assertTrue(Utils.contains(edges, new FakeEdge(
                label, outVertex, inVertex, kvs)));
    }
}
