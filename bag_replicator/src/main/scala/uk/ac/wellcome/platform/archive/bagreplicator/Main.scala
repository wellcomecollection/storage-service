package uk.ac.wellcome.platform.archive.bagreplicator

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.typesafe.config.Config
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SQSBuilder
}
import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
import uk.ac.wellcome.platform.archive.bagreplicator.bags.BagReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models.BagReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.services.BagReplicatorWorker
import uk.ac.wellcome.platform.archive.common.config.builders.{
  IngestUpdaterBuilder,
  OperationNameBuilder,
  OutgoingPublisherBuilder
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.locking.dynamo.{
  DynamoLockDao,
  DynamoLockingService
}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.typesafe.{DynamoLockDaoBuilder, S3Builder}
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

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

    implicit val s3StreamStore: S3StreamStore =
      new S3StreamStore()

    implicit val monitoringClient: CloudwatchMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: AmazonSQSAsync =
      SQSBuilder.buildSQSAsyncClient(config)

    implicit val lockDao: DynamoLockDao =
      DynamoLockDaoBuilder.buildDynamoLockDao(config)

    val operationName =
      OperationNameBuilder.getName(config)

    val lockingService =
      new DynamoLockingService[IngestStepResult[BagReplicationSummary[_]], Try]()

    val replicator = new S3Replicator()

    // Eventually this will be a config option, and each instance of
    // the replicator will choose whether it's primary/secondary,
    // and what sort of bag replicator will be passed in here.

    new BagReplicatorWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName),
      lockingService = lockingService,
      destinationConfig = ReplicatorDestinationConfig
        .buildDestinationConfig(config),
      bagReplicator = new BagReplicator(replicator)
    )
  }
}
