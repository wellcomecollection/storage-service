package uk.ac.wellcome.platform.archive.archivist.flow

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.model.PublishResult
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.platform.archive.archivist.messaging.SnsPublishFlow
import uk.ac.wellcome.platform.archive.common.models.BagRequest

object NextNotifierFlow {

  def apply(snsNextConfig: SNSConfig, snsProgressConfig: SNSConfig)(
    implicit snsClient: AmazonSNS): Flow[BagRequest, PublishResult, NotUsed] = {

    Flow[BagRequest]
      .via(
        SnsPublishFlow[BagRequest](
          snsClient,
          snsNextConfig,
          subject = "archivist_completed"))
      .log("published notification")
  }
}
