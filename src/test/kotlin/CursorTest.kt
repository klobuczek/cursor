import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.rules.TemporaryFolder
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.ResourceIterable
import org.neo4j.harness.Neo4jBuilders
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import java.util.stream.Stream

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CursorTest {
    @ParameterizedTest
    @MethodSource("service")
    fun unclosedRelationshipTraversalCursor(db: GraphDatabaseService) {
        // calls first on relationship cursor and closes the transaction
        iterate(db, Iterable<Relationship>::first)
    }

    @ParameterizedTest
    @MethodSource("service")
    fun closedRelationshipTraversalCursor(db: GraphDatabaseService) {
        // calls first on relationship cursor and closes the cursor before closing the transaction
        iterate(db) { it.use(Iterable<Relationship>::first) }
    }

    fun service() = Stream.of(
        TestDatabaseManagementServiceBuilder(TemporaryFolder().also(TemporaryFolder::create).root.toPath()).build()
            .database("neo4j"),
        Neo4jBuilders.newInProcessBuilder().build().defaultDatabaseService()
    )

    fun iterate(db: GraphDatabaseService, func: (ResourceIterable<Relationship>) -> Relationship) {
        db.beginTx().use {
            val node = it.createNode(Label.label("User"))
            node.createRelationshipTo(node, RelationshipType.withName("SELF"))
            node.relationships.let(func)
        }
    }
}