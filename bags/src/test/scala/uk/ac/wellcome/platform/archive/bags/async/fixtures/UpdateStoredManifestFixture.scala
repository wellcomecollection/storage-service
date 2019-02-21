package uk.ac.wellcome.platform.archive.bags.async.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.bags.async.services.UpdateStoredManifestService
import uk.ac.wellcome.platform.archive.bags.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait UpdateStoredManifestFixture extends SNS with StorageManifestVHSFixture {
  def withUpdateStoredManifestService[R](
    table: Table,
    bucket: Bucket,
    topic: Topic)(testWith: TestWith[UpdateStoredManifestService, R]): R =
    withStorageManifestVHS(table, bucket) { vhs =>
      withSNSWriter(topic) { progressSnsWriter =>
        val service = new UpdateStoredManifestService(
          vhs = vhs,
          progressSnsWriter = progressSnsWriter
        )
        testWith(service)
      }
    }
}
