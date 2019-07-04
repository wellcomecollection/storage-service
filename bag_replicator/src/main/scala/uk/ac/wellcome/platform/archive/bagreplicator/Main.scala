package uk.ac.wellcome.platform.archive.bagreplicator

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.services.{BagReplicator, BagReplicatorWorker}
import uk.ac.wellcome.platform.archive.common.config.builders.{IngestUpdaterBuilder, OperationNameBuilder, OutgoingPublisherBuilder}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.locking.dynamo.{DynamoLockDao, DynamoLockDaoConfig, DynamoLockingService}
import uk.ac.wellcome.storage.typesafe.{DynamoBuilder, S3Builder}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try
import uk.ac.wellcome.json.JsonUtil._
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>

    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val mat: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    val operationName =
      OperationNameBuilder.getName(config)

    // TODO: There should be a builder for this
    implicit val lockDao: DynamoLockDao = new DynamoLockDao(
      client = DynamoBuilder.buildDynamoClient(config),
      config = DynamoLockDaoConfig(
        DynamoBuilder.buildDynamoConfig(config, namespace = "locking")
      )
    )

    val lockingService =
      new DynamoLockingService[IngestStepResult[ReplicationSummary], Try]()

    new BagReplicatorWorker(
      config =
        AlpakkaSqsWorkerConfigBuilder.build(config),
      bagReplicator =
        new BagReplicator(),
      ingestUpdater =
        IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher =
        OutgoingPublisherBuilder.build(config, operationName),
      lockingService =
        lockingService,
      replicatorDestinationConfig =
        ReplicatorDestinationConfig
        .buildDestinationConfig(config)
    )
  }
}
