package uk.ac.wellcome.platform.archive.bagreplicator

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.typesafe.config.Config
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure.AzureReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.services.BagReplicatorWorker
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, AzureBlobStorageProvider, StorageProvider}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, Location, Prefix, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.locking.dynamo.{DynamoLockDao, DynamoLockingService}
import uk.ac.wellcome.storage.typesafe.{DynamoLockDaoBuilder, S3Builder}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val lockDao: DynamoLockDao =
      DynamoLockDaoBuilder.buildDynamoLockDao(config)

    val operationName =
      OperationNameBuilder.getName(config)

    val lockingService =
      new DynamoLockingService[IngestStepResult[ReplicationSummary[_]], Try]()

    val provider =
      StorageProvider.apply(config.requireString("bag-replicator.provider"))

    def createLockingService[DstPrefix <: Prefix[_ <: Location]] =
      new DynamoLockingService[IngestStepResult[ReplicationSummary[DstPrefix]], Try]()

    def createBagReplicatorWorker[DstLocation <: Location, DstPrefix <: Prefix[DstLocation]](
      lockingService: DynamoLockingService[IngestStepResult[ReplicationSummary[DstPrefix]], Try],
      replicator: Replicator[DstLocation, DstPrefix]
    ): BagReplicatorWorker[SNSConfig, SNSConfig, DstLocation, DstPrefix] =
      new BagReplicatorWorker(
        config = AlpakkaSqsWorkerConfigBuilder.build(config),
        ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
        outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
        lockingService = lockingService,
        destinationConfig = ReplicatorDestinationConfig
          .buildDestinationConfig(config),
        replicator = replicator,
        metricsNamespace = config.requireString("aws.metrics.namespace")
      )

    provider match {
      case AmazonS3StorageProvider =>
        createBagReplicatorWorker(
          lockingService = createLockingService[S3ObjectLocationPrefix],
          replicator = new S3Replicator()
        )

      case AzureBlobStorageProvider =>
        implicit val azureBlobClient: BlobServiceClient =
          new BlobServiceClientBuilder()
            .endpoint(config.requireString("azure.endpoint"))
            .buildClient()

        createBagReplicatorWorker(
          lockingService = createLockingService[AzureBlobItemLocationPrefix],
          replicator = new AzureReplicator()
        )
    }
  }
}
