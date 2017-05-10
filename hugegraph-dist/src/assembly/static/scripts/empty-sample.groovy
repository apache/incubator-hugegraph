import com.baidu.hugegraph.schema.SchemaManager
import com.baidu.hugegraph.type.schema.EdgeLabel
import com.baidu.hugegraph.type.schema.VertexLabel
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// defines a sample LifeCycleHook that prints some output to the Gremlin Server console.
// note that the name of the key in the "global" map is unimportant.
globals << [hook: [
        onStartUp : { ctx ->
            ctx.logger.info("Executed once at startup of Gremlin Server.")
            // TODO: Should remove when init.sh been added.
            graph.initBackend()

//            schema = graph.schema()
//
//            schema.makePropertyKey("name").asText().create()
//            schema.makePropertyKey("age").asInt().create()
//            schema.makePropertyKey("lang").asText().create()
//            schema.makePropertyKey("date").asText().create()
//
//            person = schema.makeVertexLabel("person").properties("name", "age").primaryKeys("name").create()
//
//            software = schema.makeVertexLabel("software").properties("name", "lang").primaryKeys("name").create()
//
//            schema.makeIndex("personByName").on(person).by("name").secondary().create()
//
//            schema.makeIndex("softwareByName").on(software).by("name").search().create()
//            schema.makeIndex("softwareByLang").on(software).by("lang").search().create()
//
//            schema.makeEdgeLabel("knows").link("person", "person").properties("date").create()
//
//            EdgeLabel created = schema.makeEdgeLabel("created").link("person", "software").properties("date").create()
//
//            schema.makeIndex("createdByDate").on(created).by("date").secondary().create()
//
//            graph.tx().open()
//
//            marko = graph.addVertex(T.label, "person", "name", "marko", "age", 29)
//            vadas = graph.addVertex(T.label, "person", "name", "vadas", "age", 27)
//            lop = graph.addVertex(T.label, "software", "name", "lop", "lang", "java")
//            josh = graph.addVertex(T.label, "person", "name", "josh", "age", 32)
//            ripple = graph.addVertex(T.label, "software", "name", "ripple", "lang", "java")
//            peter = graph.addVertex(T.label, "person", "name", "peter", "age", 35)
//
//            marko.addEdge("knows", vadas, "date", "20160110")
//            marko.addEdge("knows", josh, "date", "20130220")
//            marko.addEdge("created", lop, "date", "20171210")
//            josh.addEdge("created", ripple, "date", "20171210")
//            josh.addEdge("created", lop, "date", "20091111")
//            peter.addEdge("created", lop, "date", "20170324")
//
//            graph.tx().commit()
        },
        onShutDown: { ctx ->
            ctx.logger.info("Executed once at shutdown of Gremlin Server.")
        }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
// globals << [schemaManager: graph.openSchemaManager()]
// TODO: initBackend
globals << [schema: graph.schema()]
globals << [g: graph.traversal()]
