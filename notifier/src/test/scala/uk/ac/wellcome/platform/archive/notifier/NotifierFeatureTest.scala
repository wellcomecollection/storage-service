package uk.ac.wellcome.platform.archive.notifier

import java.net.URI

import com.github.tomakehurst.wiremock.client.WireMock.{
  equalToJson,
  postRequestedFor,
  urlPathEqualTo,
  _
}
import org.apache.http.HttpStatus
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Inside, Matchers}
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Callback,
  CallbackNotification,
  IngestCallbackStatusUpdate,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.display._
import uk.ac.wellcome.platform.archive.notifier.fixtures.{
  LocalWireMockFixture,
  NotifierFixtures
}

class NotifierFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with Akka
    with IntegrationPatience
    with LocalWireMockFixture
    with NotifierFixtures
    with Inside
    with IngestGenerators
    with TimeTestFixture
    with Eventually {

  describe("Making callbacks") {
    it("makes a POST request when it receives an Ingest with a callback") {
      withLocalWireMockClient { wireMock =>
        withNotifier {
          case (queue, _) =>
            val ingestId = createIngestID

            val callbackUri =
              new URI(s"http://$callbackHost:$callbackPort/callback/$ingestId")

            val ingest = createIngestWith(
              id = ingestId,
              callback = Some(createCallbackWith(uri = callbackUri)),
              lastModifiedDate = None
            )

            sendNotificationToSQS(
              queue,
              CallbackNotification(ingestId, callbackUri, ingest)
            )

            val expectedResponse =
              ResponseDisplayIngest(
                context = "http://localhost/context.json",
                id = ingest.id.underlying,
                sourceLocation = DisplayLocation(
                  StandardDisplayProvider,
                  ingest.sourceLocation.location.namespace,
                  ingest.sourceLocation.location.path
                ),
                callback = ingest.callback.map { DisplayCallback(_) },
                ingestType = CreateDisplayIngestType,
                space = DisplayStorageSpace(ingest.space.underlying),
                status = DisplayStatus(ingest.status.toString),
                bag = ResponseDisplayBag(
                  info = ResponseDisplayBagInfo(
                    externalIdentifier = ingest.externalIdentifier.toString,
                    version = ingest.version.map { _.toString }
                  )
                ),
                events = ingest.events.map { event =>
                  DisplayIngestEvent(
                    event.description,
                    event.createdDate.toString)
                },
                createdDate = ingest.createdDate.toString,
                lastModifiedDate = ingest.lastModifiedDate.map { _.toString }
              )

            eventually {
              wireMock.verifyThat(
                1,
                postRequestedFor(urlPathEqualTo(callbackUri.getPath))
                  .withRequestBody(equalToJson(toJson(expectedResponse).get))
              )
            }
        }
      }
    }
  }

  import org.scalatest.prop.TableDrivenPropertyChecks._

  val successfulStatuscodes =
    Table(
      "status code",
      HttpStatus.SC_OK,
      HttpStatus.SC_CREATED,
      HttpStatus.SC_ACCEPTED,
      HttpStatus.SC_NO_CONTENT
    )
  describe("Updating status") {
    it("sends an ingest when it receives a successful callback") {
      forAll(successfulStatuscodes) { statusResponse: Int =>
        withLocalWireMockClient { wireMock =>
          withNotifier {
            case (queue, messageSender) =>
              val ingestID = createIngestID

              val callbackPath = s"/callback/$ingestID"
              val callbackUri = new URI(
                s"http://$callbackHost:$callbackPort" + callbackPath
              )

              stubFor(
                post(urlEqualTo(callbackPath))
                  .willReturn(aResponse().withStatus(statusResponse))
              )

              val ingest = createIngestWith(
                id = ingestID,
                version = None,
                callback = Some(createCallbackWith(uri = callbackUri)),
                events = Seq(createIngestEvent, createIngestEvent)
              )

              sendNotificationToSQS(
                queue,
                CallbackNotification(ingestID, callbackUri, ingest)
              )

              // TODO: This test is failing because lastModifiedDate is
              // a null.  We shouldn't be sending nulls in callbacks!
              // Compare to the ingests API, and add a test for it.
              val expectedJson =
                s"""
                   |{
                   |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
                   |  "id": "${ingest.id.toString}",
                   |  "type": "Ingest",
                   |  "ingestType": {
                   |    "id": "${ingest.ingestType.id}",
                   |    "type": "IngestType"
                   |  },
                   |  "space": {
                   |    "id": "${ingest.space.underlying}",
                   |    "type": "Space"
                   |  },
                   |  "bag": {
                   |    "type": "Bag",
                   |    "info": {
                   |      "type": "BagInfo",
                   |      "externalIdentifier": "${ingest.externalIdentifier.underlying}"
                   |    }
                   |  },
                   |  "status": {
                   |    "id": "${ingest.status.toString}",
                   |    "type": "Status"
                   |  },
                   |  "sourceLocation": {
                   |    "type": "Location",
                   |    "provider": {
                   |      "type": "Provider",
                   |      "id": "aws-s3-standard"
                   |    },
                   |    "bucket": "${ingest.sourceLocation.location.namespace}",
                   |    "path": "${ingest.sourceLocation.location.path}"
                   |  },
                   |  "callback": {
                   |    "type": "Callback",
                   |    "url": "${ingest.callback.get.uri}",
                   |    "status": {
                   |      "id": "${ingest.callback.get.status.toString}",
                   |      "type": "Status"
                   |    }
                   |  },
                   |  "createdDate": "${ingest.createdDate}",
                   |  "events": [
                   |    {
                   |      "type": "IngestEvent",
                   |      "createdDate": "${ingest.events(0).createdDate}",
                   |      "description": "${ingest.events(0).description}"
                   |    },
                   |    {
                   |      "type": "IngestEvent",
                   |      "createdDate": "${ingest.events(1).createdDate}",
                   |      "description": "${ingest.events(1).description}"
                   |    }
                   |  ]
                   |}
                 """.stripMargin

              eventually {
                wireMock.verifyThat(
                  1,
                  postRequestedFor(urlPathEqualTo(callbackUri.getPath))
                    .withRequestBody(equalToJson(expectedJson))
                )

                val updates = messageSender.getMessages[IngestUpdate]
                updates should have size 1
                val receivedUpdate = updates.head

                inside(receivedUpdate) {
                  case IngestCallbackStatusUpdate(
                      id,
                      callbackStatus,
                      List(ingestEvent)) =>
                    id shouldBe ingest.id
                    ingestEvent.description shouldBe "Callback fulfilled."
                    callbackStatus shouldBe Callback.Succeeded
                    assertRecent(ingestEvent.createdDate)
                }
              }
          }
        }
      }
    }

    it(
      "sends an IngestUpdate when it receives an Ingest with a callback it cannot fulfill") {
      withNotifier {
        case (queue, messageSender) =>
          val ingestId = createIngestID

          val callbackUri = new URI(
            s"http://$callbackHost:$callbackPort/callback/$ingestId"
          )

          val ingest = createIngestWith(
            id = ingestId,
            callback = Some(createCallbackWith(uri = callbackUri))
          )

          sendNotificationToSQS[CallbackNotification](
            queue,
            CallbackNotification(ingestId, callbackUri, ingest)
          )

          eventually {
            val updates = messageSender.getMessages[IngestUpdate]
            updates should have size 1
            val receivedUpdate = updates.head

            inside(receivedUpdate) {
              case IngestCallbackStatusUpdate(
                  id,
                  callbackStatus,
                  List(ingestEvent)) =>
                id shouldBe ingest.id
                ingestEvent.description shouldBe s"Callback failed for: ${ingest.id}, got 404 Not Found!"
                callbackStatus shouldBe Callback.Failed
                assertRecent(ingestEvent.createdDate)
            }
          }
      }
    }
  }
}
