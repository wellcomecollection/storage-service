package uk.ac.wellcome.platform.archive.bagverifier.builder

import akka.actor.ActorSystem
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.services.BagVerifierWorker
import uk.ac.wellcome.platform.archive.bagverifier.services.azure.AzureReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.S3BagVerifier
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  ReplicaCompletePayload
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object BagVerifierWorkerBuilder {
  def buildBagVerifierWorker(config: Config)(
    ingestUpdater: IngestUpdater[SNSConfig],
    outgoingPublisher: OutgoingPublisher[SNSConfig]
  )(
    implicit s3: AmazonS3,
    mc: MetricsMonitoringClient,
    as: ActorSystem,
    sc: SqsAsyncClient
  ) = {
    val metricsNamespace = config.requireString("aws.metrics.namespace")

    val alpakkaSqsWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config)

    val verifierMode = config.getString("bag-verifier.mode")
    val primaryBucket =
      config.requireString("bag-verifier.primary-storage-bucket")

    verifierMode match {
      case "replica-s3" =>
        buildReplicaS3BagVerifierWorker(
          primaryBucket,
          metricsNamespace = metricsNamespace,
          alpakkaSqsWorkerConfig = alpakkaSqsWorkerConfig,
          ingestUpdater,
          outgoingPublisher
        )
      case "replica-azure" =>
        implicit val azureBlobClient: BlobServiceClient =
          new BlobServiceClientBuilder()
            .endpoint(config.requireString("azure.endpoint"))
            .buildClient()

        implicit val dynamoClient: AmazonDynamoDB =
          DynamoBuilder.buildDynamoClient(config)

        val dynamoConfig = DynamoBuilder
          .buildDynamoConfig(config, namespace = "azure_verifier_cache")

        buildReplicaAzureBagVerifierWorker(
          primaryBucket,
          dynamoConfig = dynamoConfig,
          metricsNamespace = metricsNamespace,
          alpakkaSqsWorkerConfig = alpakkaSqsWorkerConfig,
          ingestUpdater,
          outgoingPublisher
        )
      case "standalone" =>
        buildStandaloneVerifierWorker(
          primaryBucket,
          metricsNamespace = metricsNamespace,
          alpakkaSqsWorkerConfig = alpakkaSqsWorkerConfig,
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
    metricsNamespace: String,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3: AmazonS3,
    mc: MetricsMonitoringClient,
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
      metricsNamespace = metricsNamespace,
      (payload: BagRootLocationPayload) =>
        StandaloneBagVerifyContext(payload.bagRoot)
    )
  }

  def buildReplicaS3BagVerifierWorker[IngestDestination, OutgoingDestination](
    primaryBucket: String,
    metricsNamespace: String,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3: AmazonS3,
    mc: MetricsMonitoringClient,
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
      metricsNamespace = metricsNamespace,
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
    metricsNamespace: String,
    alpakkaSqsWorkerConfig: AlpakkaSQSWorkerConfig,
    ingestUpdater: IngestUpdater[IngestDestination],
    outgoingPublisher: OutgoingPublisher[OutgoingDestination]
  )(
    implicit s3Client: AmazonS3,
    blobClient: BlobServiceClient,
    dynamoClient: AmazonDynamoDB,
    mc: MetricsMonitoringClient,
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
      metricsNamespace = metricsNamespace,
      (payload: ReplicaCompletePayload) =>
        ReplicatedBagVerifyContext(
          srcRoot = payload.srcPrefix,
          replicaRoot =
            payload.dstLocation.prefix.asInstanceOf[AzureBlobLocationPrefix]
        )
    )
  }
}
