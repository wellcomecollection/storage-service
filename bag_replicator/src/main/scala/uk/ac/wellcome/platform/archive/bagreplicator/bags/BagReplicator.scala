package uk.ac.wellcome.platform.archive.bagreplicator.bags

import java.time.Instant

import org.apache.commons.io.IOUtils
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models._
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{
  ReplicationFailed,
  ReplicationResult,
  ReplicationSucceeded
}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata
import uk.ac.wellcome.storage.{Identified, ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class BagReplicator(
  replicator: Replicator
)(
  implicit
  streamStore: StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata]
) {

  def replicateBag(
    bagRequest: BagReplicationRequest
  ): Try[BagReplicationResult[BagReplicationRequest]] = {
    val startTime = Instant.now()

    val bagSummary = BagReplicationSummary(
      startTime = startTime,
      request = bagRequest
    )

    // Run the checks that a bag replica has completed successfully.
    // If checks fail, they should throw -- that causes the for comprehension
    // to stop immediately, and skip further checks.
    //
    // We catch the throwable at the bottom and wrap it back into the
    // appropriate BagReplicationFailed type.

    val result: Try[ReplicationResult] = for {
      replicationResult: ReplicationResult <- replicator.replicate(
        bagRequest.request
      ) match {
        case success: ReplicationSucceeded => Success(success)
        case ReplicationFailed(_, e)       => Failure(e)
      }

      _ <- checkTagManifestsAreTheSame(
        srcPrefix = bagRequest.request.srcPrefix,
        dstPrefix = bagRequest.request.dstPrefix
      )
    } yield replicationResult

    result.map {
      case ReplicationSucceeded(_) =>
        BagReplicationSucceeded(bagSummary.complete)

      case ReplicationFailed(_, e) =>
        BagReplicationFailed(bagSummary.complete, e)

    } recover {
      case e: Throwable =>
        BagReplicationFailed(bagSummary.complete, e)
    }
  }

  /** This step is here to check the bag created by the replica and the
    * original bag are the same; the verifier can only check that a
    * bag is correctly formed.
    *
    * Without this check, it would be possible for the replicator to
    * write an entirely different, valid bag -- and because the verifier
    * doesn't have context for the original bag, it wouldn't flag
    * it as an error.
    *
    */
  private def checkTagManifestsAreTheSame(
    srcPrefix: ObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix
  ): Try[Unit] = Try {
    val manifests =
      for {
        srcManifest <- streamStore.get(
          srcPrefix.asLocation("tagmanifest-sha256.txt")
        )
        dstManifest <- streamStore.get(
          dstPrefix.asLocation("tagmanifest-sha256.txt")
        )
      } yield (srcManifest, dstManifest)

    manifests match {
      case Right((Identified(_, srcStream), Identified(_, dstStream))) =>
        if (IOUtils.contentEquals(srcStream, dstStream)) {
          ()
        } else {
          throw new Throwable(
            "tagmanifest-sha256.txt in replica source and replica location do not match!"
          )
        }
      case err =>
        throw new Throwable(
          s"Unable to load tagmanifest-sha256.txt in source and replica to compare: $err"
        )
    }
  }
}
