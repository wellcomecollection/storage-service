package uk.ac.wellcome.platform.archive.common.storage

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{complete, get}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.StreamConverters
import org.apache.commons.io.IOUtils
import org.scalatest.FunSpec
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.S3Uploader
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.RandomThings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class LargeResponsesTest
    extends FunSpec
    with S3Fixtures
    with RandomThings
    with Akka {

  private val converter = StreamConverters.asInputStream()

  describe("LargeResponsesTest") {

    it("does not redirect a < max-length response") {
      withLocalS3Bucket { bucket =>
        withActorSystem { implicit actorSystem =>
          withMaterializer(actorSystem) { implicit mat =>
            val maxBytes = 100
            val expectedByteArray = randomBytes(maxBytes - 10)

            withLargeResponderResult(bucket, maxBytes, expectedByteArray) {
              response: HttpResponse =>
                val inputStream = response.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                response.status shouldBe StatusCodes.OK
                actualByteArray shouldBe expectedByteArray

            }
          }
        }
      }
    }

    it("redirects a > max-length response") {
      withLocalS3Bucket { bucket =>
        withActorSystem { implicit actorSystem =>
          withMaterializer(actorSystem) { implicit mat =>
            val maxBytes = 100
            val expectedByteArray = randomBytes(maxBytes + 10)

            withLargeResponderResult(bucket, maxBytes, expectedByteArray) {
              response: HttpResponse =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                val redirectLocation = response.header[Location].get

                val madeRequest = Http()
                  .singleRequest(HttpRequest(uri = redirectLocation.uri))

                val madeRequestResult: HttpResponse = Await.result(madeRequest, 10.seconds)

                madeRequestResult.status shouldBe StatusCodes.OK
                val inputStream = madeRequestResult.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                actualByteArray shouldBe expectedByteArray
            }
          }
        }
      }
    }
  }

  private def withLargeResponderResult[R](
    bucket: Bucket,
    maxBytes: Int,
    testBytes: Array[Byte]
  )(testWith: TestWith[HttpResponse, R])(
    implicit mat: ActorMaterializer,
    as: ActorSystem
  ) = {

    val uploader = new S3Uploader()

    val objectLocationPrefix =
      ObjectLocationPrefix(bucket.name, randomAlphanumeric)
    val duration = 30 seconds

    val largeResponder = new LargeResponses {
      override val s3Uploader: S3Uploader = uploader
      override val maximumResponseByteLength: Long = maxBytes
      override val prefix: ObjectLocationPrefix = objectLocationPrefix
      override val cacheDuration: Duration = duration
      override implicit val materializer: ActorMaterializer = mat
    }

    val routes = largeResponder.wrapLargeResponses(get {
      complete(testBytes)
    })

    val binding: Future[Http.ServerBinding] =
      Http().bindAndHandle(routes, "127.0.0.1", 8080)

    val madeRequest = Http()
      .singleRequest(HttpRequest(uri = "http://127.0.0.1:8080"))

    val madeRequestResult: HttpResponse = Await.result(madeRequest, 1.seconds)

    val result = testWith(madeRequestResult)

    Await
      .result(binding, 1.seconds)
      .terminate(hardDeadline = 1.seconds)
      .flatMap { _ =>
        as.terminate()
      }

    result
  }
}
