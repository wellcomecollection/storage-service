package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.worker.models.{
  NonDeterministicFailure,
  Successful
}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.storage.bag_tagger.fixtures.BagTaggerFixtures
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.tags.s3.NewS3Tags

class BagTaggerWorkerTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with BagTaggerFixtures
    with StorageManifestGenerators {
  val s3Tags = new NewS3Tags()

  val contentSha256Tags: Map[String, String] = Map(
    "Content-SHA256" -> "4a5a41ebcf5e2c24c"
  )
  val workerTestTags: Map[String, String] = Map("TagName1" -> "TagValue1")
  val tagEverythingRule
    : StorageManifest => Map[StorageManifestFile, Map[String, String]] =
    (manifest: StorageManifest) =>
      manifest.manifest.files.map { f =>
        f -> workerTestTags
      }.toMap

  describe("applies tags") {
    it("to a single location") {
      withLocalS3Bucket { replicaBucket =>
        val prefix = createS3ObjectLocationPrefixWith(replicaBucket)

        val manifest = createStorageManifestWith(
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
        createObject(location)

        val dao = createStorageManifestDao()
        dao.put(manifest) shouldBe a[Right[_, _]]

        val notification = BagRegistrationNotification(
          space = manifest.space,
          externalIdentifier = manifest.info.externalIdentifier,
          version = manifest.version.toString
        )

        withWorkerService(
          storageManifestDao = dao,
          tagRules = tagEverythingRule
        ) { worker =>
          whenReady(worker.process(notification)) {
            _ shouldBe Successful(None)
          }
        }

        s3Tags
          .get(location)
          .right
          .value shouldBe Identified(
          id = location,
          identifiedT = contentSha256Tags ++ workerTestTags
        )
      }
    }

    it("to every location on a bag") {
      withLocalS3Bucket { replicaBucket =>
        val primaryPrefix = createS3ObjectLocationPrefixWith(replicaBucket)
        val replicaPrefixes = (1 to 3).map { _ =>
          createS3ObjectLocationPrefixWith(replicaBucket)
        }

        val manifest = createStorageManifestWith(
          location = PrimaryS3StorageLocation(primaryPrefix),
          replicaLocations = replicaPrefixes.map { SecondaryS3StorageLocation },
          files = Seq(
            createStorageManifestFileWith(
              pathPrefix = "digitised/b1234",
              name = "b1234.mxf"
            )
          )
        )

        val locations =
          (replicaPrefixes :+ primaryPrefix).map { prefix =>
            prefix.asLocation("digitised/b1234", "b1234.mxf")
          }

        locations.foreach { createObject }

        val dao = createStorageManifestDao()
        dao.put(manifest) shouldBe a[Right[_, _]]

        val notification = BagRegistrationNotification(
          space = manifest.space,
          externalIdentifier = manifest.info.externalIdentifier,
          version = manifest.version.toString
        )

        withWorkerService(
          storageManifestDao = dao,
          tagRules = tagEverythingRule
        ) { worker =>
          whenReady(worker.process(notification)) {
            _ shouldBe Successful(None)
          }
        }

        locations.foreach { location =>
          s3Tags.get(location).right.value shouldBe Identified(
            id = location,
            identifiedT = contentSha256Tags ++ workerTestTags
          )
        }
      }
    }

    it("based on the supplied rules") {
      withLocalS3Bucket { replicaBucket =>
        val prefix = createS3ObjectLocationPrefixWith(replicaBucket)

        val manifest = createStorageManifestWith(
          location = PrimaryS3StorageLocation(prefix),
          replicaLocations = Seq.empty,
          files = Seq(
            createStorageManifestFileWith(
              pathPrefix = "digitised/b1234",
              name = "b1234.mxf"
            ),
            createStorageManifestFileWith(
              pathPrefix = "digitised/b5678",
              name = "b5678.mxf"
            )
          )
        )

        val location1234 = prefix.asLocation("digitised/b1234", "b1234.mxf")
        val location5678 = prefix.asLocation("digitised/b5678", "b5678.mxf")
        createObject(location1234)
        createObject(location5678)

        val dao = createStorageManifestDao()
        dao.put(manifest) shouldBe a[Right[_, _]]

        val notification = BagRegistrationNotification(
          space = manifest.space,
          externalIdentifier = manifest.info.externalIdentifier,
          version = manifest.version.toString
        )

        val rules = (manifest: StorageManifest) =>
          manifest.manifest.files.map { file =>
            val tags =
              if (file.name.endsWith("b1234.mxf")) {
                Map("b-number" -> "b1234")
              } else {
                Map("b-number" -> "b5678")
              }

            file -> tags
          }.toMap

        withWorkerService(storageManifestDao = dao, tagRules = rules) {
          worker =>
            whenReady(worker.process(notification)) {
              _ shouldBe Successful(None)
            }
        }

        s3Tags.get(location1234).right.value shouldBe Identified(
          location1234,
          contentSha256Tags ++ Map("b-number" -> "b1234")
        )
        s3Tags.get(location5678).right.value shouldBe Identified(
          location5678,
          contentSha256Tags ++ Map(
            "b-number" -> "b5678"
          )
        )
      }
    }
  }

  describe("handles errors") {
    it("if it can't read the version string as a bag version") {
      val badNotification = BagRegistrationNotification(
        space = createStorageSpace,
        externalIdentifier = createExternalIdentifier,
        version = "not-a-version-string"
      )

      withWorkerService() { worker =>
        whenReady(worker.process(badNotification)) { result =>
          result shouldBe a[NonDeterministicFailure[_]]

          val err = result.asInstanceOf[NonDeterministicFailure[_]].failure
          err shouldBe a[IllegalArgumentException]
          err.getMessage should startWith("Could not parse version string")
        }
      }
    }

    it("if it can't get the bag from the tracker") {
      val brokenDao =
        new MemoryStorageManifestDao(
          MemoryVersionedStore[BagId, StorageManifest](
            initialEntries = Map.empty
          )
        ) {
          override def get(
            id: BagId,
            version: BagVersion
          ): Either[ReadError, StorageManifest] =
            Left(StoreReadError(new Throwable("BOOM!")))
        }

      val notification = BagRegistrationNotification(
        space = createStorageSpace,
        externalIdentifier = createExternalIdentifier,
        version = createBagVersion.toString
      )

      withWorkerService(storageManifestDao = brokenDao) { worker =>
        whenReady(worker.process(notification)) { result =>
          result shouldBe a[NonDeterministicFailure[_]]

          val err = result.asInstanceOf[NonDeterministicFailure[_]].failure
          err shouldBe a[Throwable]
          err.getMessage should startWith("Unable to get bag")
        }
      }
    }

    it("if it can't apply the tags") {
      val manifest = createStorageManifest

      val dao = createStorageManifestDao()
      dao.put(manifest) shouldBe a[Right[_, _]]

      val notification = BagRegistrationNotification(
        space = manifest.space,
        externalIdentifier = manifest.info.externalIdentifier,
        version = manifest.version.toString
      )

      withWorkerService(
        storageManifestDao = dao,
        tagRules = tagEverythingRule
      ) { worker =>
        whenReady(worker.process(notification)) {
          _ shouldBe a[NonDeterministicFailure[_]]
        }
      }
    }
  }

  def createObject(location: S3ObjectLocation): Unit = {
    s3Client.putObject(
      location.bucket,
      location.key,
      "<test file contents>"
    )
    s3Tags.update(location) { _ =>
      Right(contentSha256Tags)
    }
  }
}
