package weco.storage_service.replica_aggregator

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.scanamo.generic.auto._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import weco.json.JsonUtil._
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import weco.storage_service.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import weco.storage_service.replica_aggregator.services.{
  ReplicaAggregator,
  ReplicaAggregatorWorker,
  ReplicaCounter
}
import weco.storage.dynamo.DynamoConfig
import weco.storage.store.dynamo.DynamoSingleVersionStore
import weco.storage.typesafe.DynamoBuilder
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.concurrent.ExecutionContextExecutor
import scala.language.higherKinds

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val dynamoConfig: DynamoConfig =
      DynamoBuilder.buildDynamoConfig(config, namespace = "replicas")

    implicit val dynamoClient: DynamoDbClient =
      DynamoBuilder.buildDynamoClient(config)

    val dynamoVersionedStore =
      new DynamoSingleVersionStore[ReplicaPath, AggregatorInternalRecord](
        dynamoConfig
      )

    val operationName =
      OperationNameBuilder.getName(config)

    new ReplicaAggregatorWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      replicaAggregator = new ReplicaAggregator(dynamoVersionedStore),
      replicaCounter = new ReplicaCounter(
        expectedReplicaCount =
          config.requireString("aggregator.expected_replica_count").toInt
      ),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
      metricsNamespace = config.requireString("aws.metrics.namespace")
    )
  }
}
