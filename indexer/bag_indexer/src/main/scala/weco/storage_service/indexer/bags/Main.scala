package weco.storage_service.indexer.bags

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.Index
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.elasticsearch.ElasticsearchIndexCreator
import weco.elasticsearch.typesafe.ElasticBuilder
import weco.json.JsonUtil._
import weco.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, SQSBuilder}
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.AkkaBagTrackerClient
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val metrics: CloudWatchMetrics =
      CloudWatchBuilder.buildCloudWatchMetrics(config)

    implicit val sqsClient: SqsAsyncClient =
      SqsAsyncClient.builder().build()

    val index = Index(name = config.requireString("es.bags.index-name"))
    info(s"Writing bags to index $index")

    info(s"Creating the Elasticsearch index mapping")
    val elasticClient = ElasticBuilder.buildElasticClient(config)

    val indexCreator = new ElasticsearchIndexCreator(
      elasticClient = elasticClient,
      index = index,
      config = BagsIndexConfig.config
    )

    indexCreator.create

    val bagIndexer = new BagIndexer(
      client = elasticClient,
      index = index
    )

    val bagTrackerClient = new AkkaBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    new BagIndexerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      indexer = bagIndexer,
      bagTrackerClient = bagTrackerClient
    )
  }
}
