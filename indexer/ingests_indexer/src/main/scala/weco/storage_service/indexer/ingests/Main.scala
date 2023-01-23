package weco.storage_service.indexer.ingests

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.Index
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import weco.elasticsearch.ElasticsearchIndexCreator
import weco.elasticsearch.typesafe.ElasticBuilder
import weco.json.JsonUtil._
import weco.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import weco.monitoring.cloudwatch.CloudWatchMetrics
import weco.monitoring.typesafe.CloudWatchBuilder
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

    val index = Index(name = config.requireString("es.ingests.index-name"))
    info(s"Writing ingests to index $index")

    info(s"Creating the Elasticsearch index mapping")
    val elasticClient = ElasticBuilder.buildElasticClient(config)

    val indexCreator = new ElasticsearchIndexCreator(
      elasticClient = elasticClient,
      index = index,
      config = IngestsIndexConfig.config
    )

    indexCreator.create

    val ingestIndexer = new IngestIndexer(
      client = elasticClient,
      index = index
    )

    new IngestsIndexerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      indexer = ingestIndexer
    )
  }
}
