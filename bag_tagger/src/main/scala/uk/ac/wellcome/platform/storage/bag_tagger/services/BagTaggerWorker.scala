package uk.ac.wellcome.platform.storage.bag_tagger.services

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs
import akka.stream.alpakka.sqs.MessageAction
import grizzled.slf4j.Logging
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message
import uk.ac.wellcome.messaging.sqsworker.alpakka.{AlpakkaSQSWorker, AlpakkaSQSWorkerConfig}
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Result, Successful}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.{MetricsMonitoringClient, MetricsMonitoringProcessor}
import uk.ac.wellcome.platform.archive.bag_tracker.client.BagTrackerClient
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifestFile
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class BagTaggerWorker(
  val config: AlpakkaSQSWorkerConfig,
  val metricsNamespace: String,
  bagTrackerClient: BagTrackerClient,
  applyTags: ApplyTags
)(
  implicit
  val mc: MetricsMonitoringClient,
  val as: ActorSystem,
  val sc: SqsAsyncClient,
  val wd: Decoder[BagRegistrationNotification]
) extends Runnable
    with Logging {

  implicit val ec = as.dispatcher

  def process(notification: BagRegistrationNotification): Future[Result[Unit]] = {
    val result: Future[Result[Unit]] = for {
      version <- Future.fromTry {
        BagVersion.fromString(notification.version)
      }

      bagId = BagId(
        space = notification.space,
        externalIdentifier = notification.externalIdentifier
      )

      manifest <- bagTrackerClient.getBag(bagId = bagId, version = version).map {
        case Right(bag) => bag
        case Left(err)  => throw new Throwable(s"Unable to get bag $bagId version $version from the tracker: $err")
      }

      tagsToApply: Map[StorageManifestFile, Map[String, String]] = TagRules.chooseTags(manifest)

      _ <- Future.fromTry {
        applyTags.applyTags(
          storageLocations = Seq(manifest.location) ++ manifest.replicaLocations,
          tagsToApply = tagsToApply
        )
      }

      _ = info(s"Successfully applied tags for $notification")
      result = Successful[Unit]()
    } yield result

    result
      .recover { case err: Throwable =>
        NonDeterministicFailure[Unit](err)
      }
  }

  val worker
    : AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit] =
    new AlpakkaSQSWorker[BagRegistrationNotification, Instant, Instant, Unit](
      config,
      monitoringProcessorBuilder = (ec: ExecutionContext) =>
        new MetricsMonitoringProcessor[BagRegistrationNotification](
          metricsNamespace
        )(mc, ec)
    )(process) {
      override val retryAction: Message => sqs.MessageAction =
        (message: Message) =>
          MessageAction.changeMessageVisibility(message, visibilityTimeout = 0)
    }

  override def run(): Future[Any] = worker.start
}
