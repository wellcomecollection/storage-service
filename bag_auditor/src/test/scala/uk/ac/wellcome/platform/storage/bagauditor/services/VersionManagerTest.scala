package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

trait VersionManagerFixtures extends LocalDynamoDb {
  def withVersionManager[R](table: Table)(testWith: TestWith[VersionManager, R]): R = {
    val versionManager = new VersionManager(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(versionManager)
  }

  def withVersionManager[R](testWith: TestWith[VersionManager, R]): R =
    withLocalDynamoDbTable { versionTable =>
      withVersionManager(versionTable) { versionManager =>
        testWith(versionManager)
      }
    }

  def createTable(table: Table): Table = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("externalIdentifier")
          .withKeyType(KeyType.HASH)
        )
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("version")
          .withKeyType(KeyType.RANGE)
        )
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("externalIdentifier")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("version")
            .withAttributeType("N"),
          new AttributeDefinition()
            .withAttributeName("ingestId")
            .withAttributeType("S")
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L)
        )
        .withGlobalSecondaryIndexes(
          new GlobalSecondaryIndex()
            .withIndexName(table.index)
            .withProjection(
              new Projection()
                .withProjectionType(ProjectionType.INCLUDE)
                .withNonKeyAttributes(
                  List("ingestId", "ingestDate", "externalIdentifier", "version").asJava)
            )
            .withKeySchema(
              new KeySchemaElement()
                .withAttributeName("ingestId")
                .withKeyType(KeyType.HASH),
              new KeySchemaElement()
                .withAttributeName("version")
                .withKeyType(KeyType.RANGE)
            )
            .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(1L)
              .withWriteCapacityUnits(1L))
        )

    )

    eventually {
      waitUntilActive(dynamoDbClient, table.name)
    }

    table
  }

  def assertTableHasBagVersion(table: Table, bagVersion: VersionRecord): Assertion = {
    val result: Option[Either[DynamoReadError, VersionRecord]] =
      Scanamo.get[VersionRecord](dynamoDbClient)(table.name)(
        'externalIdentifier -> bagVersion.externalIdentifier and
        'version -> bagVersion.version)

    println(Scanamo.scan[VersionRecord](dynamoDbClient)(table.name))

    result.get.right.get shouldBe bagVersion
  }
}

class VersionManagerTest extends FunSpec with Matchers with ScalaFutures with ExternalIdentifierGenerators with VersionManagerFixtures {
  it("assigns v1 for an external ID/ingest ID it's never seen before") {
    withVersionManager { versionManager =>
      (1 to 3).foreach { _ =>
        val future = versionManager.assignVersion(
          ingestId = createIngestID,
          ingestDate = Instant.now(),
          externalIdentifier = createExternalIdentifier
        )

        whenReady(future) { version =>
          version shouldBe 1
        }
      }
    }
  }

  it("records a version in DynamoDB") {
    withLocalDynamoDbTable { versionTable =>
      withVersionManager(versionTable) { versionManager =>
        val ingestId = createIngestID
        val ingestDate = Instant.now()
        val externalIdentifier = createExternalIdentifier

        assertTableEmpty[VersionRecord](versionTable)

        val future = versionManager.assignVersion(
          ingestId = ingestId,
          ingestDate = ingestDate,
          externalIdentifier = externalIdentifier
        )

        whenReady(future) { _ =>
          val expectedBagVersion = VersionRecord(
            ingestId = ingestId,
            ingestDate = ingestDate,
            externalIdentifier = externalIdentifier.underlying,
            version = 1
          )

          assertTableHasBagVersion(versionTable, expectedBagVersion)
        }
      }
    }
  }

  it("assigns sequential version numbers for the same external ID") {
    withLocalDynamoDbTable { versionTable =>
      withVersionManager(versionTable) { versionManager =>
        val externalIdentifier = createExternalIdentifier

        (1 to 5).foreach { i =>
          val future = versionManager.assignVersion(
            ingestId = createIngestID,
            ingestDate = Instant.ofEpochSecond(i),
            externalIdentifier = externalIdentifier
          )

          whenReady(future) { _ shouldBe i }
        }
      }

      Scanamo.scan[VersionRecord](dynamoDbClient)(versionTable.name) should have size 5
    }
  }

  it("always assigns the same version for a given ingest ID") {
    withLocalDynamoDbTable { versionTable =>
      withVersionManager(versionTable) { versionManager =>
        val externalIdentifier = createExternalIdentifier

        // First we store some ingest IDs in the database, and record
        // the version that were stored as.
        val ingestIds: Map[IngestID, Int] = (1 to 5).map { i =>
          val ingestId = createIngestID

          val future = versionManager.assignVersion(
            ingestId = ingestId,
            ingestDate = Instant.ofEpochSecond(i),
            externalIdentifier = externalIdentifier
          )

          ingestId -> whenReady(future) { version => version }
        }.toMap

        // Now we re-assign the version for those ingest IDs, and
        // check we get the same version back.
        ingestIds.map { case (ingestId, existingVersion) =>
          val future = versionManager.assignVersion(
            ingestId = ingestId,
            ingestDate = Instant.now(),
            externalIdentifier = externalIdentifier
          )

          whenReady(future) { _ shouldBe existingVersion }
        }
      }

      Scanamo.scan[VersionRecord](dynamoDbClient)(versionTable.name) should have size 5
    }
  }

  it("errors if you query the same ingest ID with different external identifiers") {
    withVersionManager { versionManager =>
      val ingestId = createIngestID

      val future1 = versionManager.assignVersion(
        ingestId = ingestId,
        ingestDate = Instant.now(),
        externalIdentifier = createExternalIdentifier
      )

      whenReady(future1) { _ =>
        val future2 = versionManager.assignVersion(
          ingestId = ingestId,
          ingestDate = Instant.now(),
          externalIdentifier = createExternalIdentifier
        )

        whenReady(future2.failed) { t =>
          t.getMessage should startWith("Found different external identifier")
        }
      }
    }
  }

  it("errors if you try to create a new version for an older ingest date than already stored") {
    withVersionManager { versionManager =>
      val externalIdentifier = createExternalIdentifier

      val future = versionManager.assignVersion(
        ingestId = createIngestID,
        ingestDate = Instant.ofEpochSecond(2000L),
        externalIdentifier = externalIdentifier
      )

      whenReady(future) { _ =>
        val future = versionManager.assignVersion(
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(100L),
          externalIdentifier = externalIdentifier
        )

        whenReady(future.failed) { t =>
          t.getMessage should startWith("Already assigned a version for a newer ingest")
        }
      }
    }
  }
}
