package weco.storage_service.bag_register.services

import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bag_register.fixtures.BagRegisterFixtures
import weco.storage_service.bag_register.models.RegistrationSummary
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
import weco.storage_service.bag_tracker.fixtures.BagTrackerFixtures
import weco.storage_service.bagit.models.BagId
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.generators.StorageSpaceGenerators
import weco.storage_service.storage.models._
import weco.storage.store.fixtures.StringNamespaceFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class RegisterTest
    extends AnyFunSpec
    with Matchers
    with BagRegisterFixtures
    with StorageSpaceGenerators
    with StringNamespaceFixtures
    with BagTrackerFixtures
    with ScalaFutures
    with IntegrationPatience {

  it("registers a bag with primary and secondary locations") {
    val storageManifestDao = createStorageManifestDao()

    val space = createStorageSpace
    val version = createBagVersion

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = storeS3BagWith(
        space = space,
        version = version
      )

      val primaryLocation = PrimaryS3ReplicaLocation(bagRoot)

      val replicas = collectionOf(min = 1) {
        SecondaryS3ReplicaLocation(bagRoot.copy(bucket = createBucketName))
      }

      val ingestId = createIngestID

      withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
        val register = new Register(
          bagReader = new S3BagReader(),
          bagTrackerClient = bagTrackerClient,
          storageManifestService = new S3StorageManifestService()
        )

        val future = register.update(
          ingestId = ingestId,
          location = primaryLocation,
          replicas = replicas,
          version = version,
          space = space,
          externalIdentifier = bagInfo.externalIdentifier
        )

        whenReady(future) { result =>
          result shouldBe a[IngestCompleted[_]]

          val summary = result.asInstanceOf[IngestCompleted[_]].summary
          summary.asInstanceOf[RegistrationSummary].ingestId shouldBe ingestId
        }
      }

      // Check it stores all the locations on the bag.
      val bagId = BagId(
        space = space,
        externalIdentifier = bagInfo.externalIdentifier
      )

      val manifest =
        storageManifestDao.getLatest(id = bagId).value

      manifest.location shouldBe PrimaryS3StorageLocation(
        prefix = bagRoot
          .copy(keyPrefix = bagRoot.keyPrefix.stripSuffix(s"/$version"))
      )

      manifest.replicaLocations shouldBe
        replicas.map { secondaryLocation =>
          val prefix = secondaryLocation.prefix

          SecondaryS3StorageLocation(
            prefix = prefix
              .copy(keyPrefix = prefix.keyPrefix.stripSuffix(s"/$version"))
          )
        }
    }
  }

  it("allows retrying a bag if the bag tracker is temporarily unavailable") {
    val space = createStorageSpace
    val version = createBagVersion

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = storeS3BagWith(
        space = space,
        version = version
      )

      val primaryLocation = PrimaryS3ReplicaLocation(bagRoot)

      val ingestId = createIngestID

      withActorSystem { implicit actorSystem =>
        val bagTrackerClient = new AkkaBagTrackerClient(
          trackerHost = Uri("http://localhost:9000/doesnotexist")
        )

        val register = new Register(
          bagReader = new S3BagReader(),
          bagTrackerClient = bagTrackerClient,
          storageManifestService = new S3StorageManifestService()
        )

        val future = register.update(
          ingestId = ingestId,
          location = primaryLocation,
          replicas = Seq.empty,
          version = version,
          space = space,
          externalIdentifier = bagInfo.externalIdentifier
        )

        whenReady(future) {
          _ shouldBe a[IngestShouldRetry[_]]
        }
      }
    }
  }
}
