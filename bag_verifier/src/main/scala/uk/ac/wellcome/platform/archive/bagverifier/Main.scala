package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import io.circe.Decoder
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.S3StandaloneBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagPayloadTranslator, BagVerifierWorker, ReplicatedBagRoots, StandaloneBagRoot}
import uk.ac.wellcome.platform.archive.common.{BagRootPayload, ReplicaResultPayload}
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)


    val operationName =
      OperationNameBuilder.getName(config)

    val ingestUpdater =
      IngestUpdaterBuilder.build(config, operationName)

    val outgoingPublisher =
      OutgoingPublisherBuilder.build(config, operationName)
    BagVerifierWorkerBuilder.buildBagVerifierWorker(config)(ingestUpdater,outgoingPublisher)

  }
}

object BagVerifierWorkerBuilder{
  def buildBagVerifierWorker(config: Config)(ingestUpdater: IngestUpdater[SNSConfig],outgoingPublisher:OutgoingPublisher[SNSConfig])(implicit s3: AmazonS3,mc: MetricsMonitoringClient,
                                                                                                                                     as: ActorSystem,
                                                                                                                                     sc: SqsAsyncClient) = {
    val isReplicatedBagMode = config.getBoolean("bag-verifier.mode.replicated_bag")
    isReplicatedBagMode match {
      case false => buildStandaloneVerifierWorker(config)(ingestUpdater, outgoingPublisher)
      case true => ???
    }

  }

  private def buildStandaloneVerifierWorker(config: Config)(ingestUpdater: IngestUpdater[SNSConfig],outgoingPublisher:OutgoingPublisher[SNSConfig])(implicit s3: AmazonS3,mc: MetricsMonitoringClient,
   as: ActorSystem,
   sc: SqsAsyncClient): BagVerifierWorker[BagRootPayload, StandaloneBagRoot[S3ObjectLocation, S3ObjectLocationPrefix], SNSConfig, SNSConfig] ={
    val verifier = new S3StandaloneBagVerifier(
      primaryBucket =
        config.requireString("bag-verifier.primary-storage-bucket")
    )
    new BagVerifierWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier,
      metricsNamespace = config.requireString("aws.metrics.namespace"),
     ( payload: BagRootPayload) => StandaloneBagRoot(S3ObjectLocationPrefix(payload.bagRoot))
    )

  }

}
