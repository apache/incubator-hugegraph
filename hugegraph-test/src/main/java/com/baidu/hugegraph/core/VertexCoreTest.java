package com.baidu.hugegraph.core;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.core.FakeObjects.FakeVertex;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.common.collect.ImmutableList;

public class VertexCoreTest extends BaseCoreTest {

    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        logger.info("===============  propertyKey  ================");

        schema.makePropertyKey("id").asInt().create();
        schema.makePropertyKey("name").asText().create();
        schema.makePropertyKey("dynamic").asBoolean().create();
        schema.makePropertyKey("time").asText().create();
        schema.makePropertyKey("age").asInt().valueSingle().create();
        schema.makePropertyKey("comment").asText().valueSet().create();
        schema.makePropertyKey("contribution").asText().valueSet().create();
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

        schema.makeEdgeLabel("authored").singleTime()
                .properties("contribution")
                .link("author", "book")
                .create();
        schema.makeEdgeLabel("look").multiTimes().properties("time")
                .sortKeys("time")
                .link("author", "book")
                .link("person", "book")
                .create();
        schema.makeEdgeLabel("created").singleTime()
                .link("author", "language")
                .create();
    }

    @Test
    public void testAddVertex() {
        HugeGraph graph = graph();

        // Directly into the back-end
        graph.addVertex(T.label, "book", "name", "java-3");

        graph.addVertex(T.label, "person", "name", "Baby",
                "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                "city", "Taipei", "age", 21);

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(6, count);
    }

    @Test
    public void testAddVertexWithInvalidVertexLabelType() {
        HugeGraph graph = graph();
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, true);
        });
    }

    @Test
    public void testAddVertexWithNotExistsVertexLabel() {
        HugeGraph graph = graph();
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "not-exists-label");
        });
    }

    @Test
    public void testAddVertexWithNotExistsVertexProp() {
        HugeGraph graph = graph();
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book", "not-exists-porp", "test");
        });
    }

    @Test
    public void testAddVertexWithoutPrimaryValues() {
        HugeGraph graph = graph();
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex(T.label, "book");
        });
    }

    @Test
    public void testAddVertexWithoutVertexLabel() {
        HugeGraph graph = graph();
        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph.addVertex("name", "test");
        });
    }

    @Test
    public void testAddVertexWithTx() {
        HugeGraph graph = graph();

        // tinkerpop tx
        graph.tx().open();
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");
        graph.tx().close();

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(2, count);
    }

    @Test
    public void testQueryAll() {
        HugeGraph graph = graph();
        init10Vertices();

        // query all
        List<Vertex> vertexes = graph.traversal().V().toList();

        Assert.assertEquals(10, vertexes.size());

        Assert.assertTrue(Utils.contains(vertexes,
                new FakeVertex(T.label, "author", "id", 1,
                               "name", "James Gosling", "age", 62,
                               "lived", "Canadian")));

        Assert.assertTrue(Utils.contains(vertexes,
                new FakeVertex(T.label, "language", "name", "java")));

        Assert.assertTrue(Utils.contains(vertexes,
                new FakeVertex(T.label, "book", "name", "java-1")));
    }

    @Test
    public void testQueryAllWithLimit() {
        HugeGraph graph = graph();
        init10Vertices();

        // query all with limit
        List<Vertex> vertexes = graph.traversal().V().limit(3).toList();

        Assert.assertEquals(3, vertexes.size());
    }

    @Test
    public void testQueryAllWithLimit0() {
        HugeGraph graph = graph();
        init10Vertices();

        // query all with limit 0
        List<Vertex> vertexes = graph.traversal().V().limit(0).toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryAllWithNoLimit() {
        HugeGraph graph = graph();
        init10Vertices();

        // query all with limit -1 (also no-limit)
        List<Vertex> vertexes = graph.traversal().V().limit(-1).toList();

        Assert.assertEquals(10, vertexes.size());
    }

    @Test
    public void testSplicingId() {
        HugeGraph graph = graph();
        init10Vertices();
        List<Vertex> vertexes = graph.traversal().V().toList();

        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-1")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-3")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-5")));
    }

    @Test
    public void testQueryById() {
        HugeGraph graph = graph();
        init10Vertices();

        // query vertex by id
        Id id = SplicingIdGenerator.splicing("author", "1");
        List<Vertex> vertexes = graph.traversal().V(id).toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "author", "id", 1, "name", "James Gosling",
                "age", 62, "lived", "Canadian");
    }

    @Test()
    public void testQueryByIdNotFound() {
        HugeGraph graph = graph();
        init10Vertices();

        // query vertex by id which not exists
        Id id = SplicingIdGenerator.splicing("author", "not-exists-id");
        Utils.assertThrows(HugeException.class, () -> {
            graph.traversal().V(id).toList();
        });
    }

    @Test
    public void testQueryByLabel() {
        HugeGraph graph = graph();
        init10Vertices();

        // query by vertex label
        List<Vertex> vertexes = graph.traversal().V().hasLabel("book").toList();

        Assert.assertEquals(5, vertexes.size());

        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-1")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-2")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-3")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-4")));
        Assert.assertTrue(Utils.containsId(vertexes,
                SplicingIdGenerator.splicing("book", "java-5")));
    }

    @Test
    public void testQueryByLabelAndKeyName() {
        HugeGraph graph = graph();
        init10Vertices();

        // query by vertex label and key-name
        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "language").has("dynamic").toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByPrimaryValues() {
        HugeGraph graph = graph();
        init10Vertices();

        // query vertex by primary-values
        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "author").has("id", "1").toList();
        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "author", "id", 1, "name", "James Gosling",
                "age", 62, "lived", "Canadian");
    }

    @Test
    public void testQueryFilterByPropName() {
        HugeGraph graph = graph();
        init10Vertices();

        // query vertex by condition (filter by property name)
        ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
        q.eq(HugeKeys.LABEL, "language");
        q.hasKey(HugeKeys.PROPERTIES, "dynamic");
        List<Vertex> vertexes = ImmutableList.copyOf(graph.vertices(q));

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "language", "name", "python", "dynamic", true);
    }

    @Test
    public void testQueryByStringPropWithOneResult() {
        // city is "Taipei"
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("city", "Taipei").toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "person", "name", "Hebe",
                "city", "Taipei", "age", 21);
    }

    @Test
    public void testQueryByStringPropWithMultiResults() {
        // NOTE: InMemoryDBStore would fail due to it not support index ele-ids

        // city is "Beijing"
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("city", "Beijing").toList();

        Assert.assertEquals(3, vertexes.size());

        assertContains(vertexes,
                T.label, "person", "name", "James",
                "city", "Beijing", "age", 19);
        assertContains(vertexes,
                T.label, "person", "name", "Tom Cat",
                "city", "Beijing", "age", 20);
        assertContains(vertexes,
                T.label, "person", "name", "Lisa",
                "city", "Beijing", "age", 20);
    }

    @Test
    public void testQueryByIntPropWithOneResult() {
        // age = 19
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", 19).toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "person", "name", "James",
                "city", "Beijing", "age", 19);
    }

    @Test
    public void testQueryByIntPropWithMultiResults() {
        // age = 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", 20).toList();

        Assert.assertEquals(2, vertexes.size());

        assertContains(vertexes,
                T.label, "person", "name", "Tom Cat",
                "city", "Beijing", "age", 20);
        assertContains(vertexes,
                T.label, "person", "name", "Lisa",
                "city", "Beijing", "age", 20);
    }

    @Test
    public void testQueryByIntPropWithNonResult() {
        // age = 18
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", 18).toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingLtWithOneResult() {
        // age < 19
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.lt(19)).toList();

        Assert.assertEquals(1, vertexes.size());
        assertContains(vertexes,
                T.label, "person", "name", "Baby",
                "city", "Hongkong", "age", 3);
    }

    @Test
    public void testQueryByIntPropUsingLtWithMultiResults() {
        // age < 21
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.lt(21)).toList();

        Assert.assertEquals(4, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingLteWithMultiResults() {
        // age <= 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.lte(20)).toList();

        Assert.assertEquals(4, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithOneResult() {
        // age > 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.gt(20)).toList();

        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithMultiResults() {
        // age > 1
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.gt(1)).toList();

        Assert.assertEquals(5, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGtWithNonResult() {
        // age > 30
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.gt(30)).toList();

        Assert.assertEquals(0, vertexes.size());
    }

    @Test
    public void testQueryByIntPropUsingGteWithMultiResults() {
        // age >= 20
        HugeGraph graph = graph();
        init5Persons();

        List<Vertex> vertexes = graph.traversal().V().hasLabel(
                "person").has("age", P.gte(20)).toList();

        Assert.assertEquals(3, vertexes.size());
    }

    private void init10Vertices() {
        HugeGraph graph = graph();
        graph.tx().open();

        graph.addVertex(T.label, "author", "id", 1,
                "name", "James Gosling", "age", 62, "lived", "Canadian");
        graph.addVertex(T.label, "author", "id", 2,
                "name", "Guido van Rossum", "age", 61, "lived", "California");

        graph.addVertex(T.label, "language", "name", "java");
        graph.addVertex(T.label, "language", "name", "c++");
        graph.addVertex(T.label, "language", "name", "python", "dynamic", true);

        graph.addVertex(T.label, "book", "name", "java-1");
        graph.addVertex(T.label, "book", "name", "java-2");
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.addVertex(T.label, "book", "name", "java-4");
        graph.addVertex(T.label, "book", "name", "java-5");

        graph.tx().close();
    }

    private void init5Persons() {
        HugeGraph graph = graph();
        graph.tx().open();

        graph.addVertex(T.label, "person", "name", "Baby",
                "city", "Hongkong", "age", 3);
        graph.addVertex(T.label, "person", "name", "James",
                "city", "Beijing", "age", 19);
        graph.addVertex(T.label, "person", "name", "Tom Cat",
                "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Lisa",
                "city", "Beijing", "age", 20);
        graph.addVertex(T.label, "person", "name", "Hebe",
                "city", "Taipei", "age", 21);

        graph.tx().close();
    }

    private static void assertContains(
            List<Vertex> vertexes,
            Object... keyValues) {
        Assert.assertTrue(Utils.contains(vertexes, new FakeVertex(keyValues)));
    }
}
