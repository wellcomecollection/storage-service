package uk.ac.wellcome.platform.archive.bag_tracker

import akka.http.scaladsl.model.Uri
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scalatest.concurrent.IntegrationPatience
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.client.{AkkaBagTrackerClient, BagTrackerClient, BagTrackerClientTestCases}
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.bag_tracker.storage.dynamo.DynamoStorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.services.s3.S3Uploader

class DynamoBagTrackerApiTest
  extends BagTrackerClientTestCases
    with IntegrationPatience
    with DynamoFixtures
    with S3Fixtures {
  override def withClient[R](
    trackerHost: String
  )(testWith: TestWith[BagTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaBagTrackerClient(trackerHost = Uri(trackerHost))

      testWith(client)
    }

  override def withApi[R](
    dao: StorageManifestDao
  )(testWith: TestWith[BagTrackerApi, R]): R =
    withActorSystem { implicit actorSystem =>
      val s3Uploader = new S3Uploader()

      println(dao)

      val api =
        dao match {
          case dynamoDao: DynamoStorageManifestDao =>
            new DynamoBagTrackerApi(dynamoDao, s3Uploader)(host = host, port = port)
      }

      val api = new DynamoBagTrackerApi(
        dao.asInstanceOf[DynamoStorageManifestDao],
        s3Uploader)(host = host, port = port)

      api.run()

      testWith(api)
    }

  override def withStorageManifestDao[R](
    initialManifests: Seq[StorageManifest]
  )(testWith: TestWith[StorageManifestDao, R]): R =
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        val dao = new DynamoStorageManifestDao(
          dynamoConfig = createDynamoConfigWith(table),
          s3Config = createS3ConfigWith(bucket)
        )

        initialManifests.foreach {
          dao.put(_) shouldBe a[Right[_, _]]
        }

        println(s"@@AWLC withDao = $dao")

        testWith(dao)
      }
    }

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(
      table,
      hashKeyName = "id",
      hashKeyType = ScalarAttributeType.S,
      rangeKeyName = "version",
      rangeKeyType = ScalarAttributeType.N
    )
}
