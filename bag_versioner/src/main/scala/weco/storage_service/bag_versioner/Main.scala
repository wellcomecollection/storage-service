package weco.storage_service.bag_versioner

import akka.actor.ActorSystem
import cats.Id
import com.typesafe.config.Config
import weco.json.JsonUtil._
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.storage_service.bag_versioner.services.{
  BagVersioner,
  BagVersionerWorker
}
import weco.storage_service.bag_versioner.versioning.dynamo.{
  DynamoIngestVersionManager,
  DynamoIngestVersionManagerDao
}
import weco.storage_service.bag_versioner.versioning.{
  IngestVersionManagerError,
  VersionPicker
}
import weco.storage.locking.dynamo.DynamoLockingService
import weco.storage.typesafe.{DynamoBuilder, DynamoLockDaoBuilder}
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor
import scala.language.higherKinds

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient

    implicit val lockDao = DynamoLockDaoBuilder
      .buildDynamoLockDao(config)

    val operationName = OperationNameBuilder.getName(config)

    val lockingService =
      new DynamoLockingService[
        Either[IngestVersionManagerError, BagVersion],
        Id
      ]()

    val ingestVersionManagerDao = new DynamoIngestVersionManagerDao(
      dynamoClient = DynamoBuilder.buildDynamoClient,
      dynamoConfig =
        DynamoBuilder.buildDynamoConfig(config, namespace = "versions")
    )

    val versionPicker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager =
        new DynamoIngestVersionManager(ingestVersionManagerDao)
    )

    new BagVersionerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      bagVersioner = new BagVersioner(versionPicker = versionPicker),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
      metricsNamespace = config.requireString("aws.metrics.namespace")
    )
  }
}
