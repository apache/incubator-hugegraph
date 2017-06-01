package com.baidu.hugegraph.core;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class SchemaCoreTest extends BaseCoreTest{

    // propertykey tests
    @Test
    public void testAddPropertyKeyWithoutDataType() {
        SchemaManager schema = graph().schema();
        PropertyKey id = schema.makePropertyKey("id").create();
        Assert.assertNotNull(id);
        Assert.assertEquals(DataType.TEXT, id.dataType());
    }

    @Test
    public void testAddPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.makePropertyKey("name").asText().create();

        Assert.assertNotNull(schema.propertyKey("name"));
        Assert.assertEquals("name", schema.propertyKey("name").name());
        Assert.assertEquals(DataType.TEXT,
                schema.propertyKey("name").dataType());
    }

    // vertexlabel tests
    @Test
    public void testAddVertexLabel() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(1, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
    }

    @Test
    public void testAddVertexLabelWith2PrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name", "age")
                .create();

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(2, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
        Assert.assertTrue(person.primaryKeys().contains("age"));
    }

    @Test
    public void testAddVertexLabelWithoutPrimaryKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeVertexLabel("person")
                    .properties("name", "age", "city")
                    .create();
        });
    }

    @Test
    public void testAddVertexLabelWithoutPropertyKey() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeVertexLabel("person").create();
        });
    }

    @Test
    public void testAddVertexLabelWithNotExistProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeVertexLabel("person").properties("sex").create();
        });
    }

    @Test
    public void testAddVertexLabelNewVertexWithUndefinedProperty() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "person", "name", "Baby",
                    "city", "Hongkong", "age", 3, "sex", "male");
        });
    }

    @Test
    public void testAddVertexLabelNewVertexWithPropertyAbsent() {
        initProperties();
        SchemaManager schema = graph().schema();
        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();

        graph().addVertex(T.label, "person", "name", "Baby",
                "city", "Hongkong");

        Assert.assertNotNull(person);
        Assert.assertEquals("person", person.name());
        Assert.assertEquals(3, person.properties().size());
        Assert.assertTrue(person.properties().contains("name"));
        Assert.assertTrue(person.properties().contains("age"));
        Assert.assertTrue(person.properties().contains("city"));
        Assert.assertEquals(1, person.primaryKeys().size());
        Assert.assertTrue(person.primaryKeys().contains("name"));
    }

    @Test
    public void testAddVertexLabelNewVertexWithUnmatchPropertyType() {
        initProperties();
        SchemaManager schema = graph().schema();

        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            graph().addVertex(T.label, "person", "name", "Baby",
                    "city", 2, "age", 3);
        });

    }

    // edgelabel tests
    @Test
    public void testAddEdgeLabel() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();
        schema.makeVertexLabel("book").properties("id", "name")
                .primaryKeys("id").create();
        EdgeLabel look = schema.makeEdgeLabel("look").multiTimes()
                .properties("time")
                .link("author", "book")
                .link("person", "book")
                .sortKeys("time")
                .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertEquals(2, look.links().size());
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("author", "book")));
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("person", "book")));
        Assert.assertEquals(1, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertEquals(1, look.sortKeys().size());
        Assert.assertTrue(look.sortKeys().contains("time"));
        Assert.assertEquals(Frequency.MULTIPLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutFrequency() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();
        schema.makeVertexLabel("book").properties("id", "name")
                .primaryKeys("id").create();
        EdgeLabel look = schema.makeEdgeLabel("look").properties("time")
                .link("author", "book")
                .link("person", "book")
                .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertEquals(2, look.links().size());
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("author", "book")));
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("person", "book")));
        Assert.assertEquals(1, look.properties().size());
        Assert.assertTrue(look.properties().contains("time"));
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutProperty() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();
        schema.makeVertexLabel("book").properties("id", "name")
                .primaryKeys("id").create();
        EdgeLabel look = schema.makeEdgeLabel("look").singleTime()
                .link("author", "book")
                .link("person", "book")
                .create();

        Assert.assertNotNull(look);
        Assert.assertEquals("look", look.name());
        Assert.assertEquals(2, look.links().size());
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("author", "book")));
        Assert.assertTrue(look.links()
                .contains(new EdgeLink("person", "book")));
        Assert.assertEquals(0, look.properties().size());
        Assert.assertEquals(0, look.sortKeys().size());
        Assert.assertEquals(Frequency.SINGLE, look.frequency());
    }

    @Test
    public void testAddEdgeLabelWithoutLink() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            EdgeLabel look = schema.makeEdgeLabel("look").multiTimes()
                    .properties("time")
                    .sortKeys("time")
                    .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistProperty() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeEdgeLabel("look").properties("date")
                    .link("author", "book")
                    .link("person", "book")
                    .create();
        });
    }

    @Test
    public void testAddEdgeLabelWithNotExistVertexLabel() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeEdgeLabel("look").multiTimes().properties("time")
                    .link("reviewer", "book")
                    .link("person", "book")
                    .sortKeys("time")
                    .create();
        });
    }

    @Test
    public void testAddEdgeLabelMultipleWithoutSortKey() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeEdgeLabel("look").multiTimes().properties("date")
                    .link("author", "book")
                    .link("person", "book")
                    .create();
        });
    }

    @Test
    public void testAddEdgeLabelSortKeyNotInProperty() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();

        Utils.assertThrows(IllegalArgumentException.class, () -> {
            schema.makeEdgeLabel("look").multiTimes().properties("date")
                    .link("author", "book")
                    .link("person", "book")
                    .sortKeys("time")
                    .create();
        });
    }

    // indexlabel tests
    @Test
    public void testAddIndexLabelOfVertex() {
        initProperties();
        SchemaManager schema = graph().schema();
        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .create();
        IndexLabel personByCity = schema.makeIndex("personByCity")
                .on(person).secondary().by("city").create();
        IndexLabel personByAge = schema.makeIndex("personByAge")
                .on(person).search().by("age").create();

        Assert.assertNotNull(personByCity);
        Assert.assertNotNull(personByAge);
        Assert.assertEquals(2, person.indexNames().size());
        Assert.assertTrue(person.indexNames().contains("personByCity"));
        Assert.assertTrue(person.indexNames().contains("personByAge"));
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByCity.baseType());
        Assert.assertEquals(HugeType.VERTEX_LABEL, personByAge.baseType());
        Assert.assertEquals("person", personByCity.baseValue());
        Assert.assertEquals("person", personByAge.baseValue());
        Assert.assertEquals(IndexType.SECONDARY, personByCity.indexType());
        Assert.assertEquals(IndexType.SEARCH, personByAge.indexType());
    }

    @Test
    public void testAddIndexLabelOfEdge() {
        initProperties();
        SchemaManager schema = graph().schema();
        schema.makeVertexLabel("author").properties("id", "name")
                .primaryKeys("id").create();
        schema.makeVertexLabel("book").properties("name")
                .primaryKeys("name").create();
        EdgeLabel authored = schema.makeEdgeLabel("authored").singleTime()
                .link("author", "book")
                .properties("contribution")
                .create();

        IndexLabel authoredByContri = schema.makeIndex("authoredByContri")
                .on(authored).secondary().by("city").create();

        Assert.assertNotNull(authoredByContri);
        Assert.assertEquals(1, authored.indexNames().size());
        Assert.assertTrue(authored.indexNames().contains("authoredByContri"));
        Assert.assertEquals(HugeType.EDGE_LABEL, authoredByContri.baseType());
        Assert.assertEquals("authored", authoredByContri.baseValue());
        Assert.assertEquals(IndexType.SECONDARY, authoredByContri.indexType());
    }

    // utils
    public void initProperties() {
        SchemaManager schema = graph().schema();
        schema.makePropertyKey("id").asInt().create();
        schema.makePropertyKey("name").asText().create();
        schema.makePropertyKey("age").asInt().valueSingle().create();
        schema.makePropertyKey("city").asText().create();
        schema.makePropertyKey("time").asText().create();
        schema.makePropertyKey("contribution").asText().valueSet().create();
    }
}
