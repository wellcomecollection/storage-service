package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import org.mockito.Mockito._
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{
  ReplicationFailed,
  ReplicationRequest,
  ReplicationSucceeded
}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.tags.s3.S3Tags
import uk.ac.wellcome.storage.transfer.PrefixTransfer

trait ReplicatorTestCases[
  DstNamespace,
  DstLocation <: Location,
  DstPrefix <: Prefix[DstLocation],
  PrefixTransferImpl <: PrefixTransfer[
    S3ObjectLocationPrefix,
    S3ObjectLocation,
    DstPrefix,
    DstLocation
  ]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with StorageRandomThings
    with S3Fixtures {

  def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def withDstNamespace[R](testWith: TestWith[DstNamespace, R]): R

  type ReplicatorImpl = Replicator[DstLocation, DstPrefix]

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
    val key = randomAlphanumeric
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
  val dstTags: Tags[DstLocation]

  val srcStringStore: TypedStore[S3ObjectLocation, String] =
    S3TypedStore[String]

  val dstStringStore: TypedStore[DstLocation, String]
  val dstListing: Listing[DstPrefix, DstLocation]

  def putSrcObject(location: S3ObjectLocation, contents: String): Unit =
    srcStringStore.put(location)(contents) shouldBe a[Right[_, _]]

  def putDstObject(location: DstLocation, contents: String): Unit =
    dstStringStore.put(location)(contents) shouldBe a[Right[_, _]]

  def getDstObject(location: DstLocation): String =
    dstStringStore.get(location).right.value.identifiedT

  it("replicates all the objects under a prefix") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val locations = (1 to 5).map { _ =>
          createSrcLocationWith(srcNamespace)
        }

        val objects = locations.map { _ -> randomAlphanumeric }.toMap

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

  it(
    "does not replicate objects that match the prefix but are in different directory"
  ) {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val prefix = s"v1/"
        val objectsInPrefix = (1 to 5).map { _ =>
          (
            createSrcLocationWith(srcBucket = srcNamespace, prefix = prefix),
            randomAlphanumeric
          )
        }.toMap

        val objectsDifferentPrefix = (1 to 5).map { _ =>
          (createSrcLocationWith(srcNamespace, s"v11/"), randomAlphanumeric)
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
          .toList should have size (objectsInPrefix.size)
      }
    }
  }

  it("fails if there are already different objects in the prefix") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val locations = (1 to 5).map { _ =>
          createSrcLocationWith(srcNamespace)
        }

        val objects = locations.map { _ -> randomAlphanumeric }.toMap

        objects.foreach {
          case (loc, contents) => putSrcObject(loc, contents)
        }

        val srcPrefix = createSrcPrefixWith(srcNamespace)
        val dstPrefix = createDstPrefixWith(dstNamespace)

        // Write something to the first destination.  The replicator should realise
        // this object already exists, and refuse to overwrite it.
        val badContents = randomAlphanumeric

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

  // The verifier will write a Content-SHA256 checksum tag to objects when it
  // verifies them.  If an object is then replicated to a new location, any existing
  // verification tags should be removed.
  it("doesn't copy tags from the existing objects") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val location = createSrcLocationWith(srcNamespace)

        putSrcObject(location, contents = randomAlphanumeric)
        srcTags.update(location) { existingTags =>
          Right(existingTags ++ Map("Content-SHA256" -> "abcdef"))
        }

        val request = ReplicationRequest(
          srcPrefix = createSrcPrefixWith(srcNamespace),
          dstPrefix = createDstPrefixWith(dstNamespace)
        )

        val result = withReplicator {
          _.replicate(
            ingestId = createIngestID,
            request = request
          )
        }

        result shouldBe a[ReplicationSucceeded[_]]

        val dstLocation = createDstLocationWith(
          dstNamespace,
          path = location.key
        )

        dstTags.get(dstLocation).right.value shouldBe Identified(
          dstLocation,
          Map.empty
        )
      }
    }
  }

  describe(
    "it only checks for existing objects if the destination is non-empty"
  ) {
    it("empty destination => don't check") {
      withSrcNamespace { srcNamespace =>
        withDstNamespace { dstNamespace =>
          val srcLocation = createSrcLocationWith(srcNamespace)
          putSrcObject(srcLocation, contents = randomAlphanumeric)

          val srcPrefix = createSrcPrefixWith(srcNamespace)
          val dstPrefix = createDstPrefixWith(dstNamespace)

          withPrefixTransfer { prefixTransfer =>
            val spyTransfer = spy(prefixTransfer)

            withReplicator(spyTransfer) {
              _.replicate(
                ingestId = createIngestID,
                request = ReplicationRequest(
                  srcPrefix = srcPrefix,
                  dstPrefix = dstPrefix
                )
              ) shouldBe a[ReplicationSucceeded[_]]
            }

            verify(spyTransfer, times(1))
              .transferPrefix(srcPrefix, dstPrefix, checkForExisting = false)
            verify(spyTransfer, never())
              .transferPrefix(srcPrefix, dstPrefix, checkForExisting = true)
          }
        }
      }
    }

    it("non-empty destination => check") {
      withSrcNamespace { srcNamespace =>
        withDstNamespace { dstNamespace =>
          val srcLocation = createSrcLocationWith(srcNamespace)
          putSrcObject(srcLocation, contents = randomAlphanumeric)

          val dstLocation =
            createDstLocationWith(dstNamespace, path = randomAlphanumeric)
          putDstObject(dstLocation, contents = randomAlphanumeric)

          val srcPrefix = createSrcPrefixWith(srcNamespace)
          val dstPrefix = createDstPrefixWith(dstNamespace)

          withPrefixTransfer { prefixTransfer =>
            val spyTransfer = spy(prefixTransfer)

            withReplicator(spyTransfer) {
              _.replicate(
                ingestId = createIngestID,
                request = ReplicationRequest(
                  srcPrefix = srcPrefix,
                  dstPrefix = dstPrefix
                )
              ) shouldBe a[ReplicationSucceeded[_]]
            }

            verify(spyTransfer, times(1))
              .transferPrefix(srcPrefix, dstPrefix, checkForExisting = true)
            verify(spyTransfer, never())
              .transferPrefix(srcPrefix, dstPrefix, checkForExisting = false)
          }
        }
      }
    }
  }
}
