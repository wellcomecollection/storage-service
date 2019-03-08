package uk.ac.wellcome.platform.storage.ingests.api

import java.net.URL

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import uk.ac.wellcome.messaging.sns.SNSWriter
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.typesafe.Runnable

import scala.concurrent.{ExecutionContext, Future}

class IngestsApi(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig,
  unpackerSnsWriter: SNSWriter,
  httpMetrics: HttpMetrics,
  httpServerConfig: HTTPServerConfig,
  contextURL: URL
)(implicit val actorSystem: ActorSystem,
  mat: ActorMaterializer,
  ec: ExecutionContext)
    extends Runnable {
  val ingestTracker = new IngestTracker(
    dynamoDbClient = dynamoClient,
    dynamoConfig = dynamoConfig
  )

  val router = new Router(
    ingestTracker = ingestTracker,
    ingestStarter = new IngestStarter(
      ingestTracker = ingestTracker,
      unpackerSnsWriter = unpackerSnsWriter
    ),
    httpServerConfig = httpServerConfig,
    contextURL = contextURL
  )

  val app = new WellcomeHttpApp(
    routes = router.routes,
    httpMetrics = httpMetrics,
    httpServerConfig = httpServerConfig,
    contextURL = contextURL
  )

  def run(): Future[Http.HttpTerminated] =
    app.run()
}
