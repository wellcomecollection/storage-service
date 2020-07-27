package uk.ac.wellcome.platform.storage.bag_tagger

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryS3StorageLocation,
  StorageSpace
}
import uk.ac.wellcome.platform.storage.bag_tagger.fixtures.BagTaggerFixtures
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.tags.s3.S3Tags

class BagTaggerFeatureTest
    extends AnyFunSpec
    with BagTaggerFixtures
    with Eventually
    with Matchers
    with StorageManifestGenerators {

  val s3Tags = new S3Tags()

  it("applies tags to newly-registered bags") {
    withLocalSqsQueue() { queue =>
      withLocalS3Bucket { replicaBucket =>
        val prefix = createS3ObjectLocationPrefixWith(replicaBucket)

        val manifest = createStorageManifestWith(
          space = StorageSpace("digitised"),
          location = PrimaryS3StorageLocation(prefix),
          replicaLocations = Seq.empty,
          files = Seq(
            createStorageManifestFileWith(
              pathPrefix = "digitised/b1234",
              name = "b1234.mxf"
            )
          )
        )

        val location = prefix.asLocation("digitised/b1234", "b1234.mxf")
        putStream(location)

        s3Tags.update(location) { _ =>
          Right(Map("Content-SHA256" -> "4a5a41ebcf5e2c24c"))
        }

        val dao = createStorageManifestDao()
        dao.put(manifest) shouldBe a[Right[_, _]]

        val notification = BagRegistrationNotification(manifest)

        withWorkerService(queue = queue, storageManifestDao = dao) { _ =>
          sendNotificationToSQS(queue, notification)

          eventually {
            s3Tags.get(location).right.value shouldBe Identified(
              location,
              Map(
                "Content-SHA256" -> "4a5a41ebcf5e2c24c",
                "Content-Type" -> "application/mxf"
              )
            )

            assertQueueEmpty(queue)
          }
        }
      }
    }
  }
}
