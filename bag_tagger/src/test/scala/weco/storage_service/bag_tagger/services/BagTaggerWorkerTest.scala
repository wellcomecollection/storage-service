package weco.storage_service.bag_tagger.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.worker.models.{RetryableFailure, Successful}
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.generators.StorageManifestGenerators
import weco.storage_service.storage.models._
import weco.storage_service.bag_tagger.fixtures.BagTaggerFixtures
import weco.storage.store.memory.MemoryVersionedStore
import weco.storage.tags.s3.S3Tags
import weco.storage._
import weco.storage.s3.S3ObjectLocation

import scala.util.{Failure, Try}

class BagTaggerWorkerTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with BagTaggerFixtures
    with StorageManifestGenerators {
  val s3Tags = new S3Tags()

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

        val notification = BagRegistrationNotification(manifest)

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
          replicaLocations = replicaPrefixes.map { prefix =>
            SecondaryS3StorageLocation(prefix)
          },
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

        val notification = BagRegistrationNotification(manifest)

        withWorkerService(
          storageManifestDao = dao,
          tagRules = tagEverythingRule
        ) { worker =>
          whenReady(worker.process(notification)) {
            _ shouldBe Successful(None)
          }
        }

        locations.foreach { location =>
          s3Tags.get(location).value shouldBe Identified(
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

        val notification = BagRegistrationNotification(manifest)

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

        s3Tags.get(location1234).value shouldBe Identified(
          location1234,
          contentSha256Tags ++ Map("b-number" -> "b1234")
        )
        s3Tags.get(location5678).value shouldBe Identified(
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

      // TODO: We're never going to be able to parse the version string
      // if it's unparseable, so we should consider throwing a terminal
      // failure here, rather than retrying.
      withWorkerService() { worker =>
        whenReady(worker.process(badNotification)) { result =>
          result shouldBe a[RetryableFailure[_]]

          val err = result.asInstanceOf[RetryableFailure[_]].failure
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
          result shouldBe a[RetryableFailure[_]]

          val err = result.asInstanceOf[RetryableFailure[_]].failure
          err shouldBe a[Throwable]
          err.getMessage should startWith("Unable to get bag")
        }
      }
    }

    it("if it can't apply the tags") {
      val manifest = createStorageManifest

      val dao = createStorageManifestDao()
      dao.put(manifest) shouldBe a[Right[_, _]]

      val notification = BagRegistrationNotification(manifest)

      val applyError = new Throwable("BOOM!")

      val brokenApplyTags =
        new ApplyTags(s3Tags = s3Tags) {
          override def applyTags(
            storageLocations: Seq[StorageLocation],
            tagsToApply: Map[StorageManifestFile, Map[String, String]]
          ): Try[Unit] =
            Failure(applyError)
        }

      withWorkerService(
        storageManifestDao = dao,
        tagRules = tagEverythingRule,
        applyTags = brokenApplyTags
      ) { worker =>
        whenReady(worker.process(notification)) { result =>
          result shouldBe a[RetryableFailure[_]]

          result
            .asInstanceOf[RetryableFailure[_]]
            .failure shouldBe applyError
        }
      }
    }
  }

  def createObject(location: S3ObjectLocation): Unit = {
    putStream(location)
    s3Tags.update(location) { _ =>
      Right(contentSha256Tags)
    }
  }
}
