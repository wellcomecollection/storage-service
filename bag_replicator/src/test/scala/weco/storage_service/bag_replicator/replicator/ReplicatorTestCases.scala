package weco.storage_service.bag_replicator.replicator

import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bag_replicator.replicator.models.{
  ReplicationFailed,
  ReplicationRequest,
  ReplicationSucceeded
}
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.store.TypedStore
import weco.storage.tags.Tags
import weco.storage._
import weco.storage.listing.Listing
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3TypedStore
import weco.storage.tags.s3.S3Tags
import weco.storage.transfer.PrefixTransfer

trait ReplicatorTestCases[
  DstNamespace,
  SrcLocation,
  DstLocation <: Location,
  DstPrefix <: Prefix[DstLocation],
  PrefixTransferImpl <: PrefixTransfer[
    S3ObjectLocationPrefix,
    SrcLocation,
    DstPrefix,
    DstLocation
  ]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with StorageRandomGenerators
    with S3Fixtures {

  def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def withDstNamespace[R](testWith: TestWith[DstNamespace, R]): R

  type ReplicatorImpl = Replicator[SrcLocation, DstLocation, DstPrefix]

  def withPrefixTransfer[R](testWith: TestWith[PrefixTransferImpl, R]): R

  def withReplicator[R](prefixTransferImpl: PrefixTransferImpl)(
    testWith: TestWith[ReplicatorImpl, R]
  ): R

  def withReplicator[R](testWith: TestWith[ReplicatorImpl, R]): R =
    withPrefixTransfer { prefixTransfer =>
      withReplicator(prefixTransfer) { replicator =>
        testWith(replicator)
      }
    }

  def createSrcLocationWith(
    srcBucket: Bucket,
    prefix: String = ""
  ): S3ObjectLocation = {
    val key = randomAlphanumeric()
    S3ObjectLocation(srcBucket.name, s"$prefix$key")
  }

  def createDstLocationWith(
    dstNamespace: DstNamespace,
    path: String
  ): DstLocation

  def createSrcPrefixWith(
    srcBucket: Bucket,
    keyPrefix: String = ""
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(bucket = srcBucket.name, keyPrefix = keyPrefix)

  def createDstPrefixWith(dstNamespace: DstNamespace): DstPrefix

  val srcTags: Tags[S3ObjectLocation] = new S3Tags()

  val srcStringStore: TypedStore[S3ObjectLocation, String] =
    S3TypedStore[String]

  val dstStringStore: TypedStore[DstLocation, String]
  val dstListing: Listing[DstPrefix, DstLocation]

  def putSrcObject(location: S3ObjectLocation, contents: String): Unit =
    srcStringStore.put(location)(contents) shouldBe a[Right[_, _]]

  def putDstObject(location: DstLocation, contents: String): Unit =
    dstStringStore.put(location)(contents) shouldBe a[Right[_, _]]

  def getDstObject(location: DstLocation): String =
    dstStringStore.get(location).value.identifiedT

  it("replicates all the objects under a prefix") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val locations = (1 to 5).map { _ =>
          createSrcLocationWith(srcNamespace)
        }

        val objects = locations.map { _ -> randomAlphanumeric() }.toMap

        objects.foreach {
          case (loc, contents) => putSrcObject(loc, contents)
        }

        val srcPrefix = createSrcPrefixWith(srcNamespace)
        val dstPrefix = createDstPrefixWith(dstNamespace)

        val result = withReplicator {
          _.replicate(
            ingestId = createIngestID,
            request = ReplicationRequest(
              srcPrefix = srcPrefix,
              dstPrefix = dstPrefix
            )
          )
        }

        result shouldBe a[ReplicationSucceeded[_]]
        result.summary.maybeEndTime.isDefined shouldBe true
      }
    }
  }

  // These test cases are based on a real bug we saw when adding the Azure replica.
  // We asked for a replication from `v1` (no slash), and it included objects in `v10`.
  // For example:
  //
  //       src://bags/b1234/v10/bag-info.txt
  //    ~> dst://bags/b1234/v1/0/bag-info.txt
  //
  describe("only replicates objects in the matching directory") {
    it("if the prefix has a trailing slash") {
      withSrcNamespace { srcNamespace =>
        withDstNamespace { dstNamespace =>
          val prefix = "v1/"
          val objectsInPrefix = (1 to 5).map { _ =>
            (
              createSrcLocationWith(srcBucket = srcNamespace, prefix = prefix),
              randomAlphanumeric()
            )
          }.toMap

          val objectsDifferentPrefix = (1 to 5).map { _ =>
            (createSrcLocationWith(srcNamespace, "v11/"), randomAlphanumeric())
          }.toMap

          (objectsInPrefix ++ objectsDifferentPrefix).foreach {
            case (loc, contents) => putSrcObject(loc, contents)
          }

          val srcPrefix = createSrcPrefixWith(srcNamespace, prefix)
          val dstPrefix = createDstPrefixWith(dstNamespace)

          val result = withReplicator {
            _.replicate(
              ingestId = createIngestID,
              request = ReplicationRequest(
                srcPrefix = srcPrefix,
                dstPrefix = dstPrefix
              )
            )
          }

          result shouldBe a[ReplicationSucceeded[_]]
          result.summary.maybeEndTime.isDefined shouldBe true
          dstListing
            .list(dstPrefix)
            .right
            .get
            .toList should have size objectsInPrefix.size
        }
      }
    }

    it("if the prefix omits a trailing slash") {
      withSrcNamespace { srcNamespace =>
        withDstNamespace { dstNamespace =>
          val prefix = "v1"
          val objectsInPrefix = (1 to 5).map { _ =>
            (
              createSrcLocationWith(
                srcBucket = srcNamespace,
                prefix = s"$prefix/"
              ),
              randomAlphanumeric()
            )
          }.toMap

          val objectsDifferentPrefix = (1 to 5).map { _ =>
            (
              createSrcLocationWith(srcNamespace, prefix = "v11/"),
              randomAlphanumeric()
            )
          }.toMap

          (objectsInPrefix ++ objectsDifferentPrefix).foreach {
            case (loc, contents) => putSrcObject(loc, contents)
          }

          val srcPrefix = createSrcPrefixWith(srcNamespace, prefix)
          val dstPrefix = createDstPrefixWith(dstNamespace)

          val result = withReplicator {
            _.replicate(
              ingestId = createIngestID,
              request = ReplicationRequest(
                srcPrefix = srcPrefix,
                dstPrefix = dstPrefix
              )
            )
          }

          result shouldBe a[ReplicationSucceeded[_]]
          result.summary.maybeEndTime.isDefined shouldBe true
          dstListing
            .list(dstPrefix)
            .right
            .get
            .toList should have size objectsInPrefix.size
        }
      }
    }
  }

  it("fails if there are already different objects in the prefix") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val locations = (1 to 5).map { _ =>
          createSrcLocationWith(srcNamespace)
        }

        val objects = locations.map { _ -> randomAlphanumeric() }.toMap

        objects.foreach {
          case (loc, contents) => putSrcObject(loc, contents)
        }

        val srcPrefix = createSrcPrefixWith(srcNamespace)
        val dstPrefix = createDstPrefixWith(dstNamespace)

        // Write something to the first destination.  The replicator should realise
        // this object already exists, and refuse to overwrite it.
        val badContents = randomAlphanumeric()

        val dstLocation = createDstLocationWith(
          dstNamespace = dstNamespace,
          path = locations.head.key.replace("src/", "dst/")
        )

        putDstObject(dstLocation, contents = badContents)

        val result = withReplicator {
          _.replicate(
            ingestId = createIngestID,
            request = ReplicationRequest(
              srcPrefix = srcPrefix,
              dstPrefix = dstPrefix
            )
          )
        }

        result shouldBe a[ReplicationFailed[_]]

        getDstObject(dstLocation) shouldBe badContents
      }
    }
  }

  it("fails if the underlying replication has an error") {
    val srcPrefix = withSrcNamespace { bucket =>
      createSrcPrefixWith(bucket)
    }
    val dstPrefix = withDstNamespace { namespace =>
      createDstPrefixWith(namespace)
    }

    val result = withReplicator {
      _.replicate(
        ingestId = createIngestID,
        request = ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )
    }

    result shouldBe a[ReplicationFailed[_]]
    result.summary.maybeEndTime.isDefined shouldBe true
  }

  it("checks for existing objects before replicating") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val srcLocation = createSrcLocationWith(srcNamespace)
        putSrcObject(srcLocation, contents = randomAlphanumeric())

        val dstLocation =
          createDstLocationWith(dstNamespace, path = randomAlphanumeric())
        putDstObject(dstLocation, contents = randomAlphanumeric())

        val srcPrefix = createSrcPrefixWith(srcNamespace)
        val dstPrefix = createDstPrefixWith(dstNamespace)

        withPrefixTransfer { prefixTransfer =>
          val spyTransfer = spy[PrefixTransferImpl](prefixTransfer)

          withReplicator(spyTransfer) {
            _.replicate(
              ingestId = createIngestID,
              request = ReplicationRequest(
                srcPrefix = srcPrefix,
                dstPrefix = dstPrefix
              )
            ) shouldBe a[ReplicationSucceeded[_]]
          }

          verify(spyTransfer, times(1)).transferPrefix(srcPrefix, dstPrefix)
        }
      }
    }
  }
}
