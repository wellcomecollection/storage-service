package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{
  ReplicationFailed,
  ReplicationRequest,
  ReplicationSucceeded
}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures

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
}
