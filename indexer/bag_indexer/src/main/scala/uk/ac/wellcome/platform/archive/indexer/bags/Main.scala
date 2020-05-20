package uk.ac.wellcome.platform.archive.indexer.bags

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.sksamuel.elastic4s.Index
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{AlpakkaSqsWorkerConfigBuilder, CloudwatchMonitoringClientBuilder, SQSBuilder}
import uk.ac.wellcome.messaging.worker.monitoring.metrics.cloudwatch.CloudwatchMetricsMonitoringClient
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.ElasticsearchIndexCreator
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.config.ElasticClientBuilder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContextExecutor

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem = AkkaBuilder.buildActorSystem()

    implicit val executionContext: ExecutionContextExecutor =
      actorSystem.dispatcher

    implicit val materializer: Materializer =
      AkkaBuilder.buildMaterializer()

    implicit val monitoringClient: CloudwatchMetricsMonitoringClient =
      CloudwatchMonitoringClientBuilder.buildCloudwatchMonitoringClient(config)

    implicit val sqsClient: SqsAsyncClient =
      SQSBuilder.buildSQSAsyncClient(config)

    val index = Index(name = config.required[String]("es.bags.index-name"))
    info(s"Writing bags to index $index")

    info(s"Creating the Elasticsearch index mapping")
    val elasticClient = ElasticClientBuilder.buildElasticClient(config)

    val indexCreator = new ElasticsearchIndexCreator(
      elasticClient = elasticClient
    )

    indexCreator.create(
      index = index,
      mappingDefinition = BagsIndexConfig.mapping
    )

    val bagIndexer = new BagIndexer(
      client = elasticClient,
      index = index
    )

    new BagIndexerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      indexer = bagIndexer,
      metricsNamespace = config.required[String]("aws.metrics.namespace")
    )
  }
}