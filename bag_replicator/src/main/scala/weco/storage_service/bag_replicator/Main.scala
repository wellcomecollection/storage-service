package weco.storage_service.bag_replicator

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.sns.SNSConfig
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_replicator.config.ReplicatorDestinationConfig
import weco.storage_service.bag_replicator.replicator.Replicator
import weco.storage_service.bag_replicator.replicator.azure.AzureReplicator
import weco.storage_service.bag_replicator.replicator.models.ReplicationSummary
import weco.storage_service.bag_replicator.replicator.s3.S3Replicator
import weco.storage_service.bag_replicator.services.BagReplicatorWorker
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.storage_service.ingests.models.{
  AmazonS3StorageProvider,
  AzureBlobStorageProvider,
  StorageProvider
}
import weco.storage_service.storage.models.IngestStepResult
import weco.storage.azure.AzureBlobLocationPrefix
import weco.storage.locking.dynamo.{DynamoLockDao, DynamoLockingService}
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.transfer.azure.AzurePutBlockFromUrlTransfer
import weco.storage.typesafe.{DynamoLockDaoBuilder, S3Builder}
import weco.storage.{Location, Prefix}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    import scala.concurrent.duration._

    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    implicit val lockDao: DynamoLockDao =
      DynamoLockDaoBuilder.buildDynamoLockDao(config)

    val operationName =
      OperationNameBuilder.getName(config)

    val provider =
      StorageProvider.apply(config.requireString("bag-replicator.provider"))

    def createLockingService[DstPrefix <: Prefix[_ <: Location]] =
      new DynamoLockingService[
        IngestStepResult[ReplicationSummary[DstPrefix]],
        Try]()

    def createBagReplicatorWorker[
      SrcLocation,
      DstLocation <: Location,
      DstPrefix <: Prefix[
        DstLocation
      ]
    ](
      lockingService: DynamoLockingService[IngestStepResult[
                                             ReplicationSummary[DstPrefix]
                                           ],
                                           Try],
      replicator: Replicator[SrcLocation, DstLocation, DstPrefix]
    ): BagReplicatorWorker[
      SNSConfig,
      SNSConfig,
      SrcLocation,
      DstLocation,
      DstPrefix
    ] =
      new BagReplicatorWorker(
        config = AlpakkaSqsWorkerConfigBuilder.build(config),
        ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
        outgoingPublisher =
          OutgoingPublisherBuilder.build(config, operationName),
        lockingService = lockingService,
        destinationConfig = ReplicatorDestinationConfig
          .buildDestinationConfig(config),
        replicator = replicator
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
        // The max length you can put in a single Put Block from URL API call is 100 MiB.
        // The class will load a block of this size into memory, so setting it too
        // high may cause issues.
        val blockSize: Long = 100000000L

        //Some objects can big, and take a long time to transfer, so we need to set a high validity for the s3 URL
        val s3UrlValidity = 12.hours
        createBagReplicatorWorker(
          lockingService = createLockingService[AzureBlobLocationPrefix],
          replicator = new AzureReplicator(
            transfer = AzurePutBlockFromUrlTransfer(s3UrlValidity, blockSize)
          )
        )
    }
  }
}
