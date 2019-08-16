package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{ReplicationFailed, ReplicationRequest, ReplicationSucceeded}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.{TransferFailure, TransferSuccess}
import uk.ac.wellcome.storage.transfer.s3.{S3PrefixTransfer, S3Transfer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class S3ReplicatorTest extends FunSpec with Matchers with S3Fixtures with ScalaFutures with IntegrationPatience {
  it("replicates all the objects under a prefix") {
    withLocalS3Bucket { bucket =>
      val locations = (1 to 5).map { _ =>
        createObjectLocationWith(bucket, key = s"src/$randomAlphanumeric")
      }

      val objects = locations.map { _ -> randomAlphanumeric }.toMap

      objects.foreach { case (loc, contents) =>
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

      val future = new S3Replicator().replicate(
        ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      whenReady(future) { result =>
        result shouldBe a[ReplicationSucceeded]
        result.summary.maybeEndTime.isDefined shouldBe true
      }
    }
  }

  it("fails if the underlying replication has an error") {
    val future = new S3Replicator().replicate(
      ReplicationRequest(
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

    whenReady(future) { result =>
      result shouldBe a[ReplicationFailed]
      result.summary.maybeEndTime.isDefined shouldBe true

      val failure = result.asInstanceOf[ReplicationFailed]
      failure.e shouldBe a[AmazonS3Exception]
      failure.e.getMessage should startWith("The specified bucket does not exist")
    }
  }

  it("fails if the underlying replication throws an exception") {
    val err = new Throwable("BOOM!")

    val brokenReplicator = new S3Replicator() {
      implicit val s3Transfer: S3Transfer = new S3Transfer()
      implicit val s3Listing: S3ObjectLocationListing = S3ObjectLocationListing()

      override val prefixTransfer: S3PrefixTransfer =
        new S3PrefixTransfer()(s3Transfer, s3Listing, global) {
          override def transferPrefix(srcPrefix: ObjectLocationPrefix,
                                      dstPrefix: ObjectLocationPrefix): Future[Either[TransferFailure, TransferSuccess]] =
            Future.failed(err)
        }
    }

    val future = brokenReplicator.replicate(
      ReplicationRequest(
        srcPrefix = createObjectLocationPrefix,
        dstPrefix = createObjectLocationPrefix
      )
    )

    whenReady(future) { result =>
      result shouldBe a[ReplicationFailed]
      result.summary.maybeEndTime.isDefined shouldBe true

      val failure = result.asInstanceOf[ReplicationFailed]
      failure.e shouldBe err
    }
  }
}
