package uk.ac.wellcome.platform.archive.bag_register.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.generators.StorageSpaceGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  PrimaryS3ReplicaLocation,
  SecondaryS3ReplicaLocation
}
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures

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
      val (bagRoot, bagInfo) = createRegisterBagWith(
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
        storageManifestDao.getLatest(id = bagId).right.value

      manifest.location shouldBe primaryLocation.copy(
        prefix = bagRoot
          .copy(keyPrefix = bagRoot.keyPrefix.stripSuffix(s"/$version"))
      )

      manifest.replicaLocations shouldBe
        replicas.map { secondaryLocation =>
          val prefix = secondaryLocation.prefix

          secondaryLocation.copy(
            prefix = prefix
              .copy(keyPrefix = prefix.keyPrefix.stripSuffix(s"/$version"))
          )
        }
    }
  }
}
