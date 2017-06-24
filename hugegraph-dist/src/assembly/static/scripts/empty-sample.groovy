import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// defines a sample LifeCycleHook that prints some output to the Gremlin Server console.
// note that the name of the key in the "global" map is unimportant.
globals << [hook: [
        onStartUp : { ctx ->
            ctx.logger.info("Executed once at startup of Gremlin Server.")
        },
        onShutDown: { ctx ->
            ctx.logger.info("Executed once at shutdown of Gremlin Server.")
        }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
globals << [schema: hugegraph.schema()]
globals << [g: hugegraph.traversal()]
globals << [schema1: hugegraph1.schema()]
globals << [g1: hugegraph1.traversal()]
