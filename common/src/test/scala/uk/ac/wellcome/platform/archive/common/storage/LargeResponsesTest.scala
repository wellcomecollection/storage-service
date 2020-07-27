package uk.ac.wellcome.platform.archive.common.storage

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{ETag, Location}
import akka.http.scaladsl.model.{
  HttpHeader,
  HttpRequest,
  HttpResponse,
  StatusCodes
}
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.stream.scaladsl.StreamConverters
import org.apache.commons.io.IOUtils
import org.scalatest.funspec.AnyFunSpec
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3Uploader
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class LargeResponsesTest
    extends AnyFunSpec
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

    describe("redirects a > max-length response") {
      it("without Etag") {
        withLocalS3Bucket { bucket =>
          withActorSystem { implicit actorSystem =>
            withMaterializer(actorSystem) { implicit mat =>
              val maxBytes = 100
              val expectedByteArray = randomBytes(maxBytes + 10)
              val prefix = randomAlphanumeric

              withLargeResponderResult(
                bucket,
                maxBytes,
                expectedByteArray,
                prefix
              ) { response: HttpResponse =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                val redirectLocation = response.header[Location].get

                val madeRequest = Http()
                  .singleRequest(HttpRequest(uri = redirectLocation.uri))

                val madeRequestResult: HttpResponse =
                  Await.result(madeRequest, 10.seconds)

                madeRequestResult.status shouldBe StatusCodes.OK
                val inputStream = madeRequestResult.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                actualByteArray shouldBe expectedByteArray

                val keys = listKeysInBucket(bucket)
                keys should have length (1)

                val keyParts = keys.head.split("/")
                keyParts should have length (2)

                keyParts.head shouldBe prefix
                UUID.fromString(keyParts(1)) shouldBe a[UUID]
              }
            }
          }
        }
      }

      it("with Etag") {
        withLocalS3Bucket { bucket =>
          withActorSystem { implicit actorSystem =>
            withMaterializer(actorSystem) { implicit mat =>
              val maxBytes = 100
              val expectedByteArray = randomBytes(maxBytes + 10)
              val header = ETag(randomAlphanumeric)
              val prefix = randomAlphanumeric

              withLargeResponderResult(
                bucket,
                maxBytes,
                expectedByteArray,
                prefix,
                List(header)
              ) { response: HttpResponse =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                val redirectLocation = response.header[Location].get

                val madeRequest = Http()
                  .singleRequest(HttpRequest(uri = redirectLocation.uri))

                val madeRequestResult: HttpResponse =
                  Await.result(madeRequest, 10.seconds)

                madeRequestResult.status shouldBe StatusCodes.OK
                val inputStream = madeRequestResult.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                actualByteArray shouldBe expectedByteArray

                val keys = listKeysInBucket(bucket)

                keys should have length (1)

                val keyParts = keys.head.split("/")
                keyParts should have length (2)

                keyParts.head shouldBe prefix
                keyParts.tail.head shouldBe header.value().replace("\"", "")
              }
            }
          }
        }
      }
    }
  }

  private def withLargeResponderResult[R](
    bucket: Bucket,
    maxBytes: Int,
    testBytes: Array[Byte],
    prefix: String = randomAlphanumeric,
    headers: List[HttpHeader] = Nil
  )(testWith: TestWith[HttpResponse, R])(
    implicit mat: Materializer,
    as: ActorSystem
  ): R = {

    val port = 8080
    val interface = "127.0.0.1"

    val uploader = new S3Uploader()

    val duration = 30 seconds

    val largeResponder = new LargeResponses {
      override val s3Uploader: S3Uploader = uploader
      override val s3Prefix: S3ObjectLocationPrefix =
        S3ObjectLocationPrefix(
          bucket = bucket.name,
          keyPrefix = prefix
        )

      override val maximumResponseByteLength: Long = maxBytes
      override val cacheDuration: Duration = duration
      override implicit val materializer: Materializer = mat
    }

    val routes = largeResponder.wrapLargeResponses(get {
      respondWithHeaders(headers) {
        complete(testBytes)
      }
    })

    val binding: Future[Http.ServerBinding] =
      Http().bindAndHandle(routes, interface, port)

    val madeRequest = Http().singleRequest(
      HttpRequest(uri = s"http://$interface:$port")
    )

    // This can sometimes flake out when running in Travis CI.  The deadline
    // is somewhat arbitrary, so if it keeps failing, bump this deadline again.
    val madeRequestResult: HttpResponse =
      Await.result(madeRequest, atMost = 5.seconds)

    val result = testWith(madeRequestResult)

    Await
      .result(binding, atMost = 5.seconds)
      .terminate(hardDeadline = 5.seconds)
      .flatMap { _ =>
        as.terminate()
      }

    result
  }
}
