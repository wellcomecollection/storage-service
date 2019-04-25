//package uk.ac.wellcome.platform.archive.bagreplicator
//
//import akka.actor.ActorSystem
//import akka.stream.ActorMaterializer
//import com.amazonaws.services.s3.AmazonS3
//import com.amazonaws.services.sqs.AmazonSQSAsync
//import com.typesafe.config.Config
//import uk.ac.wellcome.messaging.typesafe.{
//  CloudwatchMonitoringClientBuilder,
//  SQSBuilder
//}
//import uk.ac.wellcome.messaging.worker.monitoring.CloudwatchMonitoringClient
//import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
//import uk.ac.wellcome.platform.archive.bagreplicator.services.{
//  BagReplicator,
//  BagReplicatorWorker
//}
//import uk.ac.wellcome.platform.archive.bagunpacker.config.builders.AlpakkaSqsWorkerConfigBuilder
//import uk.ac.wellcome.platform.archive.common.config.builders.{
//  IngestUpdaterBuilder,
//  OperationNameBuilder,
//  OutgoingPublisherBuilder
//}
//import uk.ac.wellcome.storage.typesafe.S3Builder
//import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
//import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
//
//import scala.concurrent.ExecutionContextExecutor
//
//object Main extends WellcomeTypesafeApp {
//  runWithConfig { config: Config =>
//    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()
//    implicit val executionContext: ExecutionContextExecutor =
//      actorSystem.dispatcher
//    implicit val materializer: ActorMaterializer =
//      AkkaBuilder.buildActorMaterializer()
//
//    implicit val s3Client: AmazonS3 = S3Builder.buildS3Client(config)
//
//    implicit val monitoringClient: CloudwatchMonitoringClient =
//      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)
//
//    implicit val sqsClient: AmazonSQSAsync =
//      SQSBuilder.buildSQSAsyncClient(config)
//
//    val operationName = OperationNameBuilder
//      .getName(config, default = "replicating")
//
//    new BagReplicatorWorker(
//      alpakkaSQSWorkerConfig = AlpakkaSqsWorkerConfigBuilder.build(config),
//      bagReplicator = new BagReplicator(
//        config = ReplicatorDestinationConfig
//          .buildDestinationConfig(config)
//      ),
//      ingestUpdater = IngestUpdaterBuilder.build(config, operationName),
//      outgoingPublisher = OutgoingPublisherBuilder.build(config, operationName)
//    )
//  }
//}
