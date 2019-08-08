package uk.ac.wellcome.platform.archive.common.fixtures

import java.net.URL

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Sink
import io.circe.Decoder
import org.scalatest.{Assertion, Matchers}
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.http.models.{
  InternalServerErrorResponse,
  UserErrorResponse
}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait HttpFixtures extends Akka with ScalaFutures with Matchers {
  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
  import uk.ac.wellcome.json.JsonUtil._

  private def whenRequestReady[R](
    r: HttpRequest
  )(testWith: TestWith[HttpResponse, R]): R =
    withActorSystem { implicit actorSystem =>
      val request = Http().singleRequest(r)
      whenReady(request) { response: HttpResponse =>
        testWith(response)
      }
    }

  def whenGetRequestReady[R](
    path: String
  )(testWith: TestWith[HttpResponse, R]): R =
    whenRequestReady(HttpRequest(GET, path)) { response =>
      testWith(response)
    }

  def whenPostRequestReady[R](url: String, entity: RequestEntity)(
    testWith: TestWith[HttpResponse, R]
  ): R = {
    val request = HttpRequest(
      method = POST,
      uri = url,
      headers = Nil,
      entity = entity
    )

    whenRequestReady(request) { response =>
      testWith(response)
    }
  }

  def getT[T](entity: HttpEntity)(implicit decoder: Decoder[T]): T =
    withMaterializer { implicit materializer =>
      val timeout = 300.millis

      val stringBody = entity
        .toStrict(timeout)
        .map(_.data)
        .map(_.utf8String)
        .value
        .get
        .get
      fromJson[T](stringBody).get
    }

  def withStringEntity[R](
    httpEntity: HttpEntity
  )(testWith: TestWith[String, R]): R =
    withMaterializer { implicit materializer =>
      val value =
        httpEntity.dataBytes.runWith(Sink.fold("") {
          case (acc, byteString) =>
            acc + byteString.utf8String
        })
      whenReady(value) { string =>
        testWith(string)
      }
    }

  def createHTTPServerConfig: HTTPServerConfig =
    HTTPServerConfig(
      host = "localhost",
      port = 1234,
      externalBaseURL = "http://localhost:1234"
    )

  val httpServerConfig: HTTPServerConfig = createHTTPServerConfig

  val metricsName: String

  def assertMetricSent(
    metrics: MemoryMetrics[_],
    result: HttpMetricResults.Value
  ): Assertion =
    metrics.incrementedCounts should contain(
      s"${metricsName}_HttpResponse_$result"
    )

  val contextURL: URL

  def assertIsUserErrorResponse(
    response: HttpResponse,
    description: String,
    statusCode: StatusCode = StatusCodes.BadRequest,
    label: String = "Bad Request"
  ): Assertion =
    withMaterializer { implicit materializer =>
      response.status shouldBe statusCode
      response.entity.contentType shouldBe ContentTypes.`application/json`

      val ingestFuture = Unmarshal(response.entity).to[UserErrorResponse]

      whenReady(ingestFuture) { actualError =>
        actualError shouldBe UserErrorResponse(
          context = contextURL.toString,
          httpStatus = statusCode.intValue,
          description = description,
          label = label
        )
      }
    }

  def assertIsInternalServerErrorResponse(response: HttpResponse): Assertion =
    withMaterializer { implicit materializer =>
      response.status shouldBe StatusCodes.InternalServerError
      response.entity.contentType shouldBe ContentTypes.`application/json`

      val ingestFuture = Unmarshal(response.entity)
        .to[InternalServerErrorResponse]

      whenReady(ingestFuture) { actualError =>
        actualError shouldBe InternalServerErrorResponse(
          context = contextURL.toString,
          httpStatus = StatusCodes.InternalServerError.intValue,
          label = StatusCodes.InternalServerError.reason
        )
      }
    }
}
