package weco.storage_service.bag_versioner

import org.apache.pekko.actor.ActorSystem
import cats.Id
import com.typesafe.config.Config
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.PekkoSQSWorkerConfigBuilder
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage.locking.dynamo.DynamoLockingService
import weco.storage.typesafe.{DynamoBuilder, DynamoLockDaoBuilder}
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
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.typesafe.WellcomeTypesafeApp

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val ec: ExecutionContext =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    implicit val lockDao = DynamoLockDaoBuilder
      .buildDynamoLockDao(config)

    val operationName = OperationNameBuilder.getName(config)

    val lockingService =
      new DynamoLockingService[
        Either[IngestVersionManagerError, BagVersion],
        Id
      ]()

    val ingestVersionManagerDao = new DynamoIngestVersionManagerDao(
      dynamoClient = DynamoDbClient.builder().build(),
      dynamoConfig =
        DynamoBuilder.buildDynamoConfig(config, namespace = "versions")
    )

    val versionPicker = new VersionPicker(
      lockingService = lockingService,
      ingestVersionManager =
        new DynamoIngestVersionManager(ingestVersionManagerDao)
    )

    new BagVersionerWorker(
      config = PekkoSQSWorkerConfigBuilder.build(config),
      bagVersioner = new BagVersioner(versionPicker = versionPicker),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName)
    )
  }
}
