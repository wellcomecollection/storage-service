package uk.ac.wellcome.platform.archive.bagreplicator.bags

import java.time.Instant

import uk.ac.wellcome.platform.archive.bagreplicator.bags.models._
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{
  ReplicationFailed,
  ReplicationResult,
  ReplicationSucceeded
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID

import scala.util.{Failure, Success, Try}

class BagReplicator(
  replicator: Replicator
) {
  def replicateBag(
    ingestId: IngestID,
    bagRequest: BagReplicationRequest
  ): Try[BagReplicationResult[BagReplicationRequest]] = {
    val startTime = Instant.now()

    val bagSummary = BagReplicationSummary(
      ingestId = ingestId,
      startTime = startTime,
      request = bagRequest
    )

    val result: Try[ReplicationResult] = for {
      replicationResult: ReplicationResult <- replicator.replicate(
        ingestId = ingestId,
        request = bagRequest.request
      ) match {
        case success: ReplicationSucceeded => Success(success)
        case ReplicationFailed(_, e)       => Failure(e)
      }
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
}
