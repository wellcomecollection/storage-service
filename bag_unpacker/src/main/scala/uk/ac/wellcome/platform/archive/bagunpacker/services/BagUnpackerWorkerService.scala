package uk.ac.wellcome.platform.archive.bagunpacker.services

import akka.Done
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.messaging.sqs.NotificationStream
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, UnpackBagRequest}
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BagUnpackerWorkerService(
                                stream: NotificationStream[UnpackBagRequest],
                                progressSnsWriter: SNSWriter,
                                outgoingSnsWriter: SNSWriter
) extends Logging
    with Runnable {

  def run(): Future[Done] =
    stream.run(processMessage)

  def processMessage(
    unpackBagRequest: UnpackBagRequest
  ): Future[Unit] =
    outgoingSnsWriter
      .writeMessage(
        BagRequest(
          unpackBagRequest.requestId,
          // TODO: The unpacker will need some more info
          // namespace/prefix from config.
          // externalIdentifier from bag-info.txt
          BagLocation(
            storageNamespace = "uploadNamespace",
            storagePrefix = Some("uploadPrefix"),
            unpackBagRequest.storageSpace,
            bagPath = BagPath("externalIdentifier")
          )
        ),
        subject = s"Sent by ${this.getClass.getSimpleName}"
      )
      .map(_ => ())

}
