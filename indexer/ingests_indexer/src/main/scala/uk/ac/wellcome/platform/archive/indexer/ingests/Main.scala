package uk.ac.wellcome.platform.archive.indexer.ingests

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.sksamuel.elastic4s.Index
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{
  AlpakkaSqsWorkerConfigBuilder,
  CloudwatchMonitoringClientBuilder,
  SQSBuilder
}
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

    // Ingests will be written a lot when they're initially processing, then never written
    // again once the ingest completes.  Lots of writes upfront, then read-only.
    //
    // For this reason, we do initial writes into per-day indexes, and then aggregate them
    // into a single index with a rollup job (https://www.elastic.co/guide/en/kibana/current/data-rollups.html)
    //
    // The per-day indexes are small and handle the intensive writes; the long-term index
    // only gets written to by the rollup job.
    //
    // Because the app will scale to zero when it's not running, it's okay if the
    // index name only changes when the app starts.
    //
    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE
    val currentWeek = dateFormatter.format(LocalDate.now())
    val indexPrefix: String = config.required[String]("es.ingests.index-prefix")
    val indexName = s"$indexPrefix--$currentWeek"
    val index = Index(indexName)

    info(s"Writing ingests to per-week index $index")

    info(s"Creating the Elasticsearch index mapping")
    val elasticClient = ElasticClientBuilder.buildElasticClient(config)

    val indexCreator = new ElasticsearchIndexCreator(
      elasticClient = elasticClient
    )

    indexCreator.create(
      index = index,
      mappingDefinition = IngestsIndexConfig.mapping
    )

    val ingestIndexer = new IngestIndexer(
      client = elasticClient,
      index = index
    )

    new IngestsIndexerWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestIndexer = ingestIndexer,
      metricsNamespace = config.required[String]("aws.metrics.namespace")
    )
  }
}
