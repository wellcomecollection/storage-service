package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sns.AmazonSNS
import grizzled.slf4j.Logging
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.platform.archive.bagreplicator.config.BagVerifierConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global

class BagVerifier(
  s3Client: AmazonS3,
  snsClient: AmazonSNS,
  bagVerifierConfig: BagVerifierConfig,
  progressSnsConfig: SNSConfig,
  outgoingSnsConfig: SNSConfig
)(
  implicit val actorSystem: ActorSystem
) extends Logging
    with Runnable {

  def run(): Future[Unit] = Future {


    ()
  }
}
