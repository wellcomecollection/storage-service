package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepResult,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.transfer.PrefixTransfer
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.concurrent.{ExecutionContext, Future}

class BagReplicator(
  implicit
  prefixTransfer: PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
  ec: ExecutionContext
) extends Logging {

  def replicate(
    srcPrefix: ObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix
  ): Future[IngestStepResult[ReplicationSummary]] = {
    val replicationSummary = ReplicationSummary(
      startTime = Instant.now(),
      srcPrefix = srcPrefix,
      dstPrefix = dstPrefix
    )

    val future =
      prefixTransfer.transferPrefix(
        srcPrefix = srcPrefix,
        dstPrefix = dstPrefix
      )

    future
      .map {
        case Right(_) =>
          IngestStepSucceeded(
            replicationSummary.complete
          )

        case Left(storageError) =>
          error("Storage error while replicating", storageError.e)
          IngestFailed(
            replicationSummary.complete,
            storageError.e
          )
      }
      .recover { case err =>
        error("Storage error while replicating", err)
        IngestFailed(
          replicationSummary.complete,
          err
        )
      }
  }
}
