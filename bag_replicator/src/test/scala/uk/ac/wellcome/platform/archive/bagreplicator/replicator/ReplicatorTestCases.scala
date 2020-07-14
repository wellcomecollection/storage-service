package uk.ac.wellcome.platform.archive.bagreplicator.replicator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{ReplicationFailed, ReplicationRequest, ReplicationSucceeded}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait ReplicatorTestCases[SrcNamespace, DstNamespace]
  extends AnyFunSpec
    with Matchers
    with StorageRandomThings {
  def withSrcNamespace[R](testWith: TestWith[SrcNamespace, R]): R
  def withDstNamespace[R](testWith: TestWith[DstNamespace, R]): R

  def withReplicator[R](testWith: TestWith[Replicator, R]): R

  def createSrcLocationWith(srcNamespace: SrcNamespace): ObjectLocation
  def createDstLocationWith(dstNamespace: DstNamespace, path: String): ObjectLocation

  def createSrcPrefixWith(srcNamespace: SrcNamespace): ObjectLocationPrefix
  def createDstPrefixWith(dstNamespace: DstNamespace): ObjectLocationPrefix

  def putSrcObject(location: ObjectLocation, contents: String): Unit
  def putDstObject(location: ObjectLocation, contents: String): Unit

  def getDstObject(location: ObjectLocation): String

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

        result shouldBe a[ReplicationSucceeded]
        result.summary.maybeEndTime.isDefined shouldBe true
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
          path = locations.head.path.replace("src/", "dst/")
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

        result shouldBe a[ReplicationFailed]

        getDstObject(dstLocation) shouldBe badContents
      }
    }
  }
}
