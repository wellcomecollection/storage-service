package weco.storage_service.bag_verifier.builder

import akka.actor.ActorSystem
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.sns.SNSConfig
import weco.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.monitoring.Metrics
import weco.storage_service.bag_verifier.models.{
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import weco.storage_service.bag_verifier.services.BagVerifierWorker
import weco.storage_service.bag_verifier.services.azure.AzureReplicatedBagVerifier
import weco.storage_service.bag_verifier.services.s3.S3BagVerifier
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services.OutgoingPublisher
import weco.storage_service.{BagRootLocationPayload, ReplicaCompletePayload}
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.dynamo.DynamoConfig
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.typesafe.DynamoBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.Future

object BagVerifierWorkerBuilder {
  def buildBagVerifierWorker(config: Config)(
    ingestUpdater: IngestUpdater[SNSConfig],
    outgoingPublisher: OutgoingPublisher[SNSConfig]
  )(
    implicit s3: S3Client,
    metrics: Metrics[Future],
    as: ActorSystem,
    sc: SqsAsyncClient
  ) = {
    val alpakkaSqsWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config)

    val verifierMode = config.getString("bag-verifier.mode")
    val primaryBucket =
      config.requireString("bag-verifier.primary-storage-bucket")

    verifierMode match {
      case "replica-s3" =>
        buildReplicaS3BagVerifierWorker(
          primaryBucket,
          alpakkaSqsWorkerConfig,
          ingestUpdater,
          outgoingPublisher
        )
      case "replica-azure" =>
        implicit val azureBlobClient: BlobServiceClient =
          new BlobServiceClientBuilder()
            .endpoint(config.requireString("azure.endpoint"))
            .buildClient()

        implicit val dynamoClient: DynamoDbClient =
          DynamoBuilder.buildDynamoClient

        val dynamoConfig = DynamoBuilder
          .buildDynamoConfig(config, namespace = "azure_verifier_cache")

        buildReplicaAzureBagVerifierWorker(
          primaryBucket,
          dynamoConfig,
          alpakkaSqsWorkerConfig,
          ingestUpdater,
          outgoingPublisher
        )
      case "standalone" =>
        buildStandaloneVerifierWorker(
          primaryBucket,
          alpakkaSqsWorkerConfig,
          ingestUpdater,
          outgoingPublisher
        )
      case _ =>
        throw new Exception(
          s"$verifierMode is not a valid value for bag-verifier.mode"
        )
    }
  }

  def buildStandaloneVerifierWorker[IngestDestination, OutgoingDestination](
    primaryBucket: String,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3: S3Client,
    metrics: Metrics[Future],
    as: ActorSystem,
    sc: SqsAsyncClient
  ): BagVerifierWorker[
    S3ObjectLocation,
    S3ObjectLocationPrefix,
    StandaloneBagVerifyContext,
    BagRootLocationPayload,
    IngestDestination,
    OutgoingDestination
  ] = {
    val verifier = S3BagVerifier.standalone(primaryBucket)

    new BagVerifierWorker(
      config = alpakkaSqsWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier,
      (payload: BagRootLocationPayload) =>
        StandaloneBagVerifyContext(payload.bagRoot)
    )
  }

  def buildReplicaS3BagVerifierWorker[IngestDestination, OutgoingDestination](
    primaryBucket: String,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3: S3Client,
    metrics: Metrics[Future],
    as: ActorSystem,
    sc: SqsAsyncClient
  ): BagVerifierWorker[
    S3ObjectLocation,
    S3ObjectLocationPrefix,
    ReplicatedBagVerifyContext[S3ObjectLocationPrefix],
    ReplicaCompletePayload,
    IngestDestination,
    OutgoingDestination
  ] = {
    val verifier = S3BagVerifier.replicated(primaryBucket)
    new BagVerifierWorker(
      config = alpakkaSqsWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier,
      (payload: ReplicaCompletePayload) =>
        ReplicatedBagVerifyContext(
          srcRoot = payload.srcPrefix,
          replicaRoot =
            payload.dstLocation.prefix.asInstanceOf[S3ObjectLocationPrefix]
      )
    )
  }

  def buildReplicaAzureBagVerifierWorker[
    IngestDestination,
    OutgoingDestination
  ](
    primaryBucket: String,
    dynamoConfig: DynamoConfig,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3Client: S3Client,
    blobClient: BlobServiceClient,
    dynamoClient: DynamoDbClient,
    metrics: Metrics[Future],
    as: ActorSystem,
    sc: SqsAsyncClient
  ): BagVerifierWorker[
    AzureBlobLocation,
    AzureBlobLocationPrefix,
    ReplicatedBagVerifyContext[AzureBlobLocationPrefix],
    ReplicaCompletePayload,
    IngestDestination,
    OutgoingDestination
  ] = {
    val verifier = AzureReplicatedBagVerifier(
      primaryBucket = primaryBucket,
      dynamoConfig = dynamoConfig
    )
    new BagVerifierWorker(
      config = alpakkaSqsWorkerConfig,
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier,
      (payload: ReplicaCompletePayload) =>
        ReplicatedBagVerifyContext(
          srcRoot = payload.srcPrefix,
          replicaRoot =
            payload.dstLocation.prefix.asInstanceOf[AzureBlobLocationPrefix]
      )
    )
  }
}
