package uk.ac.wellcome.platform.archive.bag_register.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.bag_register.models.RegistrationSummary
import uk.ac.wellcome.platform.archive.bag_register.services.s3.S3StorageManifestService
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.generators.{NewStorageLocationGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.models.AmazonS3StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class RegisterTest
    extends AnyFunSpec
    with Matchers
    with BagRegisterFixtures
    with StorageSpaceGenerators
    with NewStorageLocationGenerators
    with StringNamespaceFixtures
    with BagTrackerFixtures
    with ScalaFutures
    with IntegrationPatience {

  it("registers a bag with primary and secondary locations") {
    val storageManifestService = new S3StorageManifestService()

    val storageManifestDao = createStorageManifestDao()

    val space = createStorageSpace
    val version = createBagVersion

    withLocalS3Bucket { implicit bucket =>
      val (bagRoot, bagInfo) = createRegisterBagWith(
        space = space,
        version = version
      )

      val primaryLocation = PrimaryS3StorageLocation(prefix = bagRoot)

      val replicas = collectionOf(min = 1) {
        SecondaryS3StorageLocation(
          prefix = bagRoot.copy(bucket = createBucketName)
        )
      }

      val ingestId = createIngestID

      withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
        val register = new Register(
          bagReader = new S3BagReader(),
          bagTrackerClient = bagTrackerClient,
          storageManifestService = storageManifestService,
          toPrefix = (prefix: ObjectLocationPrefix) =>
            S3ObjectLocationPrefix(prefix.namespace, prefix.path)
        )

        val future = register.update(
          ingestId = ingestId,
          location = PrimaryStorageLocation(
            provider = AmazonS3StorageProvider,
            prefix = primaryLocation.prefix.toObjectLocationPrefix
          ),
          replicas = replicas.map { secondaryLocation =>
            SecondaryStorageLocation(
              provider = AmazonS3StorageProvider,
              prefix = secondaryLocation.prefix.toObjectLocationPrefix
            )
          },
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
          .copy(
            keyPrefix = bagRoot.pathPrefix.stripSuffix(s"/$version")
          )
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

  it(
    "includes a user-facing message if the fetch.txt refers to the wrong namespace"
  ) {
    val storageManifestService = new S3StorageManifestService()

    val space = createStorageSpace
    val version = createBagVersion

    val storageManifestDao = createStorageManifestDao()

    withLocalS3Bucket { implicit bucket =>
      val (bagObjects, bagRoot, bagInfo) =
        withNamespace { implicit namespace =>
          createBagContentsWith(
            version = version
          )
        }

      // Actually upload the bag objects into a different namespace,
      // so the entries in the fetch.txt will be wrong.
      val badBagObjects = bagObjects.map {
        case (objLocation, contents) =>
          objLocation.copy(bucket = objLocation.bucket + "-wrong") -> contents
      }

      uploadBagObjects(bagRoot = bagRoot, objects = badBagObjects)

      val location = PrimaryS3StorageLocation(
        prefix = bagRoot
          .copy(
            bucket = bagRoot.bucket + "-wrong"
          )
      )

      withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
        val register = new Register(
          bagReader = new S3BagReader(),
          bagTrackerClient = bagTrackerClient,
          storageManifestService = storageManifestService,
          toPrefix = (prefix: ObjectLocationPrefix) =>
            S3ObjectLocationPrefix(prefix.namespace, prefix.path)
        )

        val future = register.update(
          ingestId = createIngestID,
          location = PrimaryStorageLocation(
            provider = AmazonS3StorageProvider,
            prefix = location.prefix.toObjectLocationPrefix
          ),
          replicas = Seq.empty,
          version = version,
          space = space,
          externalIdentifier = bagInfo.externalIdentifier
        )

        whenReady(future) { result =>
          result shouldBe a[IngestFailed[_]]

          val ingestFailed = result.asInstanceOf[IngestFailed[_]]
          ingestFailed.e shouldBe a[BadFetchLocationException]
          ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
            """Fetch entry for data/[0-9A-Za-z/]+ refers to a file in the wrong namespace: [0-9A-Za-z/]+"""
        }
      }
    }
  }
}
