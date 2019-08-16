package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagreplicator.models._
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{ReplicationRequest, ReplicationResult}
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded}
import uk.ac.wellcome.storage.transfer.{PrefixTransfer, TransferResult}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.concurrent.{ExecutionContext, Future}

sealed trait BagReplicator extends Replicator {
  def replicateBag(
                    srcPrefix: ObjectLocationPrefix,
                    dstPrefix: ObjectLocationPrefix
                  ): IngestStepResult[ReplicationRequest]
}

class PrimaryBagReplicator()(
  implicit val prefixTransfer: PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
  implicit val ec: ExecutionContext
) extends BagReplicator {

  override def replicateBag(
                             srcPrefix: ObjectLocationPrefix,
                             dstPrefix: ObjectLocationPrefix
                           ): IngestStepResult[ReplicationResult] = {

    replicate(ReplicationRequest(srcPrefix, dstPrefix))
  }

}





//  def replicateBag(
//                 srcPrefix: ObjectLocationPrefix,
//                 dstPrefix: ObjectLocationPrefix
//               ): Future[IngestStepResult[ReplicationSummary]] = {
//    val replicationSummary = Replication(
//      startTime = Instant.now(),
//      srcPrefix = srcPrefix,
//      dstPrefix = dstPrefix
//    )
//
//    val future =
//      prefixTransfer.transferPrefix(
//        srcPrefix = srcPrefix,
//        dstPrefix = dstPrefix
//      )
//
//    future
//      .map {
//        case Right(_) =>
//          IngestStepSucceeded(
//            replicationSummary.complete
//          )
//
//        case Left(storageError: TransferResult) =>
//          error("Storage error while replicating", storageError.e)
//          IngestFailed(
//            replicationSummary.complete,
//            storageError.e
//          )
//      }
//      .recover {
//        case err: Throwable =>
//          error("Storage error while replicating", err)
//          IngestFailed(
//            replicationSummary.complete,
//            err
//          )
//      }
//  }
//}
//
//class PrimaryBagReplicator()(implicit val prefixTransfer: PrefixTransfer[ObjectLocationPrefix, ObjectLocation],
//                             implicit val ec: ExecutionContext) extends BagReplicator {
//
//}
