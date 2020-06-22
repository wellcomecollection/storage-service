package uk.ac.wellcome.platform.storage.replica_aggregator

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SQSBuilder
}
import uk.ac.wellcome.platform.archive.common.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import uk.ac.wellcome.platform.storage.replica_aggregator.services.{
  ReplicaAggregator,
  ReplicaAggregatorWorker,
  ReplicaCounter
}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.store.dynamo.DynamoSingleVersionStore
import uk.ac.wellcome.storage.typesafe.DynamoBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._
import org.scanamo.auto._
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val mat: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val dynamoConfig: DynamoConfig =
      DynamoBuilder.buildDynamoConfig(config, namespace = "replicas")

    implicit val dynamoClient: AmazonDynamoDB =
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
      // TODO: Make this configurable
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
