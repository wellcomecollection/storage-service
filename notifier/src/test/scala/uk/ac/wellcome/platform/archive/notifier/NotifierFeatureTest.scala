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
              callback = Some(createCallbackWith(uri = callbackUri))
            )

            sendNotificationToSQS(
              queue,
              CallbackNotification(ingestId, callbackUri, ingest)
            )

            eventually {
              wireMock.verifyThat(
                1,
                postRequestedFor(urlPathEqualTo(callbackUri.getPath))
                  .withRequestBody(equalToJson(toJson(ResponseDisplayIngest(
                    "http://localhost/context.json",
                    ingest.id.underlying,
                    DisplayLocation(
                      StandardDisplayProvider,
                      ingest.sourceLocation.location.namespace,
                      ingest.sourceLocation.location.key),
                    ingest.callback.map(DisplayCallback(_)),
                    CreateDisplayIngestType,
                    DisplayStorageSpace(ingest.space.underlying),
                    DisplayStatus(ingest.status.toString),
                    ingest.bag.map(bagId =>
                      ResponseDisplayIngestBag(
                        s"${bagId.space}/${bagId.externalIdentifier}")),
                    ingest.events.map(event =>
                      DisplayIngestEvent(
                        event.description,
                        event.createdDate.toString)),
                    ingest.createdDate.toString,
                    ingest.lastModifiedDate.toString
                  )).get))
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
    it("sends an IngestUpdate when it receives a successful callback") {
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
                callback = Some(createCallbackWith(uri = callbackUri))
              )

              sendNotificationToSQS(
                queue,
                CallbackNotification(ingestID, callbackUri, ingest)
              )

              eventually {
                wireMock.verifyThat(
                  1,
                  postRequestedFor(urlPathEqualTo(callbackUri.getPath))
                    .withRequestBody(equalToJson(toJson(ResponseDisplayIngest(
                      "http://localhost/context.json",
                      ingest.id.underlying,
                      DisplayLocation(
                        StandardDisplayProvider,
                        ingest.sourceLocation.location.namespace,
                        ingest.sourceLocation.location.key),
                      ingest.callback.map(DisplayCallback(_)),
                      DisplayIngestType(ingest.ingestType),
                      DisplayStorageSpace(ingest.space.underlying),
                      DisplayStatus(ingest.status.toString),
                      ingest.bag.map(bagId =>
                        ResponseDisplayIngestBag(
                          s"${bagId.space}/${bagId.externalIdentifier}")),
                      ingest.events.map(event =>
                        DisplayIngestEvent(
                          event.description,
                          event.createdDate.toString)),
                      ingest.createdDate.toString,
                      ingest.lastModifiedDate.toString
                    )).get))
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
