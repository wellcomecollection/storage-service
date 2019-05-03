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
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

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
            .withAttributeType("N")
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L)
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
}
