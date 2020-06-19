package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{ReplicationFailed, ReplicationRequest, ReplicationSucceeded}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.tags.s3.S3Tags
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

class S3ReplicatorTest
    extends AnyFunSpec
    with Matchers
    with S3Fixtures
    with StorageRandomThings {
  it("replicates all the objects under a prefix") {
    withLocalS3Bucket { bucket =>
      val locations = (1 to 5).map { _ =>
        createObjectLocationWith(bucket, key = s"src/$randomAlphanumeric")
      }

      val objects = locations.map { _ -> randomAlphanumeric }.toMap

      objects.foreach {
        case (loc, contents) =>
          s3Client.putObject(
            loc.namespace,
            loc.path,
            contents
          )
      }

      val srcPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "src/"
      )

      val dstPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "dst/"
      )

      val result = new S3Replicator().replicate(
        ingestId = createIngestID,
        request = ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      result shouldBe a[ReplicationSucceeded]
      result.summary.maybeEndTime.isDefined shouldBe true
    }
  }

  it("fails if there are already different objects in the prefix") {
    withLocalS3Bucket { bucket =>
      val locations = (1 to 5).map { _ =>
        createObjectLocationWith(bucket, key = s"src/$randomAlphanumeric")
      }

      val objects = locations.map { _ -> randomAlphanumeric }.toMap

      objects.foreach {
        case (loc, contents) =>
          s3Client.putObject(
            loc.namespace,
            loc.path,
            contents
          )
      }

      val srcPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "src/"
      )

      val dstPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "dst/"
      )

      // Write something to the first destination.  The replicator should realise
      // this object already exists, and refuse to overwrite it.
      val badContents = randomAlphanumeric

      val dstLocation = ObjectLocation(
        namespace = bucket.name,
        path = locations.head.path.replace("src/", "dst/")
      )

      s3Client.putObject(dstLocation.namespace, dstLocation.path, badContents)

      val result = new S3Replicator().replicate(
        ingestId = createIngestID,
        request = ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      result shouldBe a[ReplicationFailed]

      getContentFromS3(dstLocation) shouldBe badContents
    }
  }

  it("fails if the underlying replication has an error") {
    val result = new S3Replicator().replicate(
      ingestId = createIngestID,
      request = ReplicationRequest(
        srcPrefix = ObjectLocationPrefix(
          namespace = createBucketName,
          path = randomAlphanumeric
        ),
        dstPrefix = ObjectLocationPrefix(
          namespace = createBucketName,
          path = randomAlphanumeric
        )
      )
    )

    result shouldBe a[ReplicationFailed]
    result.summary.maybeEndTime.isDefined shouldBe true

    val failure = result.asInstanceOf[ReplicationFailed]
    failure.e shouldBe a[AmazonS3Exception]
    failure.e.getMessage should startWith(
      "The specified bucket does not exist"
    )
  }

  // The verifier will write a Content-SHA256 checksum tag to objects when it
  // verifies them.  If an object is then replicated to a new location, any existing
  // verification tags should be removed.
  it("doesn't copy tags from the existing objects") {
    val s3Tags = new S3Tags()

    withLocalS3Bucket { srcBucket =>
      withLocalS3Bucket { dstBucket =>
        val location = createObjectLocationWith(srcBucket)

        s3Client.putObject(
          location.namespace,
          location.path,
          randomAlphanumeric
        )
        s3Tags.update(location) { existingTags =>
          Right(existingTags ++ Map("Content-SHA256" -> "abcdef"))
        }

        val request = ReplicationRequest(
          srcPrefix =
            ObjectLocationPrefix(namespace = srcBucket.name, path = ""),
          dstPrefix =
            ObjectLocationPrefix(namespace = dstBucket.name, path = "")
        )

        val result = new S3Replicator().replicate(
          ingestId = createIngestID,
          request = request
        )

        result shouldBe a[ReplicationSucceeded]

        val dstLocation = location.copy(namespace = dstBucket.name)
        s3Tags.get(dstLocation).right.value shouldBe  Identified(dstLocation, Map.empty)
      }
    }
  }
}
