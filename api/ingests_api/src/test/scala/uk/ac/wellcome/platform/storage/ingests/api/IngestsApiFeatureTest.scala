package uk.ac.wellcome.platform.storage.ingests.api

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.optics.JsonPath.root
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Inside, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.{IngestID, IngestRequestPayload}
import uk.ac.wellcome.platform.archive.display._
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestsApiFixture
import uk.ac.wellcome.storage.ObjectLocation

class IngestsApiFeatureTest
    extends FunSpec
    with Matchers
    with IngestsApiFixture
    with Inside
    with IntegrationPatience
    with JsonAssertions
    with TimeTestFixture {

  val contextUrl = "http://api.wellcomecollection.org/storage/v1/context.json"
  describe("GET /ingests/:id") {
    it("returns a ingest tracker when available") {
      withConfiguredApp {
        case (ingestTracker, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            val ingest = createIngest

            val expectedSourceLocationJson =
              s"""{
                  "provider": {
                    "id": "${StandardDisplayProvider.id}",
                    "type": "Provider"
                  },
                  "bucket": "${ingest.sourceLocation.location.namespace}",
                  "path": "${ingest.sourceLocation.location.key}",
                  "type": "Location"
                }""".stripMargin

            val expectedCallbackJson =
              s"""{
                  "url": "${ingest.callback.get.uri}",
                  "status": {
                    "id": "processing",
                    "type": "Status"
                  },
                  "type": "Callback"
                }""".stripMargin

            val expectedIngestTypeJson =
              s"""{
                "id": "create",
                "type": "IngestType"
              }""".stripMargin

            val expectedSpaceJson =
              s"""{
                "id": "${ingest.space.underlying}",
                "type": "Space"
              }""".stripMargin

            val expectedStatusJson =
              s"""{
                "id": "accepted",
                "type": "Status"
              }""".stripMargin

            ingestTracker.initialise(ingest)

            whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") {
              result =>
                result.status shouldBe StatusCodes.OK

                withStringEntity(result.entity) { jsonString =>
                  val json = parse(jsonString).right.get
                  root.`@context`.string
                    .getOption(json)
                    .get shouldBe "http://api.wellcomecollection.org/storage/v1/context.json"
                  root.id.string
                    .getOption(json)
                    .get shouldBe ingest.id.toString

                  assertJsonStringsAreEqual(
                    root.sourceLocation.json.getOption(json).get.noSpaces,
                    expectedSourceLocationJson)

                  assertJsonStringsAreEqual(
                    root.callback.json.getOption(json).get.noSpaces,
                    expectedCallbackJson)
                  assertJsonStringsAreEqual(
                    root.ingestType.json.getOption(json).get.noSpaces,
                    expectedIngestTypeJson)
                  assertJsonStringsAreEqual(
                    root.space.json.getOption(json).get.noSpaces,
                    expectedSpaceJson)
                  assertJsonStringsAreEqual(
                    root.status.json.getOption(json).get.noSpaces,
                    expectedStatusJson)
                  assertJsonStringsAreEqual(
                    root.events.json.getOption(json).get.noSpaces,
                    "[]")

                  root.`type`.string.getOption(json).get shouldBe "Ingest"

                  assertRecent(
                    Instant.parse(
                      root.createdDate.string.getOption(json).get),
                    25)
                  assertRecent(
                    Instant.parse(
                      root.lastModifiedDate.string.getOption(json).get),
                    25)
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
            }
          }
      }
    }

    it("does not output empty values") {
      withConfiguredApp {
        case (ingestTracker, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            val ingest = createIngestWith(callback = None)
            ingestTracker.initialise(ingest)
            whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") {
              result =>
                result.status shouldBe StatusCodes.OK
                withStringEntity(result.entity) { jsonString =>
                  val infoJson = parse(jsonString).right.get
                  infoJson.findAllByKey("callback") shouldBe empty
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
            }
          }
      }
    }

    it("returns a 404 NotFound if no ingest tracker matches id") {
      withMaterializer { implicit materializer =>
        withConfiguredApp {
          case (_, _, metricsSender, baseUrl) =>
            val id = randomUUID
            whenGetRequestReady(s"$baseUrl/ingests/$id") { response =>
              assertIsUserErrorResponse(
                response,
                description = s"Ingest $id not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.UserError)
            }
        }
      }
    }

    it("returns a 500 Server Error if reading from DynamoDB fails") {
      withMaterializer { implicit materializer =>
        withBrokenApp {
          case (_, _, metricsSender, baseUrl) =>
            whenGetRequestReady(s"$baseUrl/ingests/$randomUUID") { response =>
              assertIsInternalServerErrorResponse(response)

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.ServerError)
            }
        }
      }
    }
  }

  describe("POST /ingests") {
    it("creates an ingest") {
      withConfiguredApp {
        case (ingestTracker, unpackerMessageSender, metricsSender, baseUrl) =>
          withMaterializer { implicit mat =>
            val url = s"$baseUrl/ingests"

            val bucketName = "bucket"
            val s3key = "key.txt"
            val spaceName = "somespace"

            val entity = HttpEntity(
              ContentTypes.`application/json`,
              s"""|{
                 |  "type": "Ingest",
                 |  "ingestType": {
                 |    "id": "create",
                 |    "type": "IngestType"
                 |  },
                 |  "sourceLocation":{
                 |    "type": "Location",
                 |    "provider": {
                 |      "type": "Provider",
                 |      "id": "${StandardDisplayProvider.id}"
                 |    },
                 |    "bucket": "$bucketName",
                 |    "path": "$s3key"
                 |  },
                 |  "space": {
                 |    "id": "$spaceName",
                 |    "type": "Space"
                 |  },
                 |  "callback": {
                 |    "url": "${testCallbackUri.toString}"
                 |  }
                 |}""".stripMargin
            )

            val expectedLocationR = s"$baseUrl/(.+)".r

            whenPostRequestReady(url, entity) { response: HttpResponse =>
              response.status shouldBe StatusCodes.Created

              val maybeId = response.headers.collectFirst {
                case HttpHeader("location", expectedLocationR(id)) => id
              }

              maybeId.isEmpty shouldBe false
              val id = UUID.fromString(maybeId.get)

              val ingestFuture =
                Unmarshal(response.entity).to[ResponseDisplayIngest]

              whenReady(ingestFuture) { actualIngest =>
                inside(actualIngest) {
                  case ResponseDisplayIngest(
                      actualContextUrl,
                      actualId,
                      actualSourceLocation,
                      Some(
                        DisplayCallback(
                          actualCallbackUrl,
                          Some(DisplayStatus(actualCallbackStatus, "Status")),
                          "Callback")),
                      CreateDisplayIngestType,
                      DisplayStorageSpace(actualSpaceId, "Space"),
                      DisplayStatus("accepted", "Status"),
                      None,
                      Nil,
                      actualCreatedDate,
                      actualLastModifiedDate,
                      "Ingest") =>
                    actualContextUrl shouldBe contextUrl
                    actualId shouldBe id
                    actualSourceLocation shouldBe DisplayLocation(
                      StandardDisplayProvider,
                      bucketName,
                      s3key)
                    actualCallbackUrl shouldBe testCallbackUri.toString
                    actualCallbackStatus shouldBe "processing"
                    actualSpaceId shouldBe spaceName

                    val expectedIngest = Ingest(
                      id = IngestID(id),
                      sourceLocation = StorageLocation(
                        StandardStorageProvider,
                        ObjectLocation(bucketName, s3key)
                      ),
                      space = Namespace(spaceName),
                      callback =
                        Some(Callback(testCallbackUri, Callback.Pending)),
                      status = Ingest.Accepted,
                      bag = None,
                      createdDate = Instant.parse(actualCreatedDate),
                      lastModifiedDate = Instant.parse(actualLastModifiedDate),
                      events = Nil
                    )

                    ingestTracker.ingests shouldBe Map(expectedIngest.id -> expectedIngest)

                    val expectedPayload = IngestRequestPayload(expectedIngest)
                    unpackerMessageSender.getMessages[IngestRequestPayload]() shouldBe Seq(expectedPayload)
                }
              }

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.Success)
            }
          }
      }
    }

    describe("returns a 400 error for malformed requests") {
      it("if the ingest request doesn't have a sourceLocation") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materializer =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """|{
                   |  "type": "Ingest",
                   |  "ingestType": {
                   |    "id": "create",
                   |    "type": "IngestType"
                   |  },
                   |  "space": {
                   |    "id": "bcnfgh",
                   |    "type": "Space"
                   |  }
                   |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation: required property not supplied."
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the body of the request is not valid JSON") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """hgjh""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "The request content was malformed:\nexpected json value got h (line 1, column 1)"
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the Content-Type of the request is not an accepted type") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`text/plain(UTF-8)`,
                """hgjh""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "The request's Content-Type is not supported. Expected:\napplication/json",
                  statusCode = StatusCodes.UnsupportedMediaType,
                  label = "Unsupported Media Type"
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the ingest request doesn't have a sourceLocation or ingestType") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """|{
                   |  "type": "Ingest",
                   |  "space": {
                   |    "id": "bcnfgh",
                   |    "type": "Space"
                   |  }
                   |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """|Invalid value at .sourceLocation: required property not supplied.
                       |Invalid value at .ingestType: required property not supplied.""".stripMargin
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the sourceLocation doesn't have a bucket field") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "${StandardDisplayProvider.id}"
                    |    },
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  }
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation.bucket: required property not supplied."
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the sourceLocation has an invalid bucket field") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "${StandardDisplayProvider.id}"
                    |    },
                    |    "bucket": {"name": "bucket"},
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  }
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation.bucket: should be a String."
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the provider doesn't have a valid id field") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """|{
                   |  "type": "Ingest",
                   |  "ingestType": {
                   |    "id": "create",
                   |    "type": "IngestType"
                   |  },
                   |  "sourceLocation":{
                   |    "type": "Location",
                   |    "provider": {
                   |      "type": "Provider",
                   |      "id": "blipbloop"
                   |    },
                   |    "bucket": "bucket",
                   |    "path": "b22454408.zip"
                   |  },
                   |  "space": {
                   |    "id": "bcnfgh",
                   |    "type": "Space"
                   |  }
                   |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """Invalid value at .sourceLocation.provider.id: got "blipbloop", valid values are: aws-s3-standard, aws-s3-ia."""
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the ingestType doesn't have a valid id field") {
        withConfiguredApp {
          case (_, unpackerMessageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """|{
                   |  "type": "Ingest",
                   |  "ingestType": {
                   |    "id": "baboop",
                   |    "type": "IngestType"
                   |  },
                   |  "sourceLocation":{
                   |    "type": "Location",
                   |    "provider": {
                   |      "type": "Provider",
                   |      "id": "aws-s3-standard"
                   |    },
                   |    "bucket": "bucket",
                   |    "path": "b22454408.zip"
                   |  },
                   |  "space": {
                   |    "id": "bcnfgh",
                   |    "type": "Space"
                   |  }
                   |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """Invalid value at .ingestType.id: got "baboop", valid values are: create."""
                )

                unpackerMessageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }
    }

    it("returns a 500 Server Error if updating DynamoDB fails") {
      withMaterializer { implicit materializer =>
        withBrokenApp {
          case (_, _, metricsSender, baseUrl) =>
            val entity = HttpEntity(
              ContentTypes.`application/json`,
              s"""|{
                  |  "type": "Ingest",
                  |  "ingestType": {
                  |    "id": "create",
                  |    "type": "IngestType"
                  |  },
                  |  "sourceLocation":{
                  |    "type": "Location",
                  |    "provider": {
                  |      "type": "Provider",
                  |      "id": "${StandardDisplayProvider.id}"
                  |    },
                  |    "bucket": "bukkit",
                  |    "path": "key"
                  |  },
                  |  "space": {
                  |    "id": "space",
                  |    "type": "Space"
                  |  },
                  |  "callback": {
                  |    "url": "${testCallbackUri.toString}"
                  |  }
                  |}""".stripMargin
            )

            whenPostRequestReady(s"$baseUrl/ingests/$randomUUID", entity) {
              response =>
                assertIsInternalServerErrorResponse(response)

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.ServerError)
            }
        }
      }
    }
  }

  describe("GET /ingests/find-by-bag-id/:bag-id") {
    it("returns a list of ingests for the given bag id") {
      withConfiguredApp {
        case (ingestTracker, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            val ingest = createIngest
            ingestTracker.initialise(ingest)
            val bagId = createBagId

            val storedIngest = ingestTracker.update(
              IngestStatusUpdate(
                id = ingest.id,
                status = Ingest.Accepted,
                affectedBag = Some(bagId)
              )
            ).get

            val bagIngest = BagIngest(
              id = ingest.id,
              bagIdIndex = bagId.toString,
              createdDate = storedIngest.lastModifiedDate
            )

            whenGetRequestReady(s"$baseUrl/ingests/find-by-bag-id/$bagId") {
              response =>
                response.status shouldBe StatusCodes.OK
                response.entity.contentType shouldBe ContentTypes.`application/json`
                val displayBagIngestFutures =
                  Unmarshal(
                    response.entity
                  ).to[List[DisplayIngestMinimal]]

                whenReady(displayBagIngestFutures) { displayBagIngests =>
                  displayBagIngests shouldBe List(
                    DisplayIngestMinimal(bagIngest))
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success
                )
            }
          }
      }
    }

    it("returns a list of ingests for the given bag id with : separated parts") {
      withConfiguredApp {
        case (ingestTracker, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            val ingest = createIngest
            ingestTracker.initialise(ingest)
            val bagId = createBagId
            val bagIngest = BagIngest(
              id = ingest.id,
              bagIdIndex = bagId.toString,
              createdDate = Instant.now
            )

            ingestTracker.update(
              IngestStatusUpdate(
                id = ingest.id,
                status = Ingest.Accepted,
                affectedBag = Some(bagId)
              )
            )

            whenGetRequestReady(
              s"$baseUrl/ingests/find-by-bag-id/${bagId.space}:${bagId.externalIdentifier}") {
              response =>
                response.status shouldBe StatusCodes.OK
                response.entity.contentType shouldBe ContentTypes.`application/json`
                val displayBagIngestFutures =
                  Unmarshal(response.entity).to[List[DisplayIngestMinimal]]

                whenReady(displayBagIngestFutures) { displayBagIngests =>
                  displayBagIngests shouldBe List(
                    DisplayIngestMinimal(bagIngest))

                  assertMetricSent(
                    metricsSender,
                    result = HttpMetricResults.Success
                  )
                }
            }
          }
      }
    }

    it("returns 'Not Found' if there are no ingests for the given bag id") {
      withConfiguredApp {
        case (_, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            whenGetRequestReady(s"$baseUrl/ingests/find-by-bag-id/$randomUUID") {
              response =>
                response.status shouldBe StatusCodes.NotFound
                response.entity.contentType shouldBe ContentTypes.`application/json`
                val displayBagIngestFutures =
                  Unmarshal(response.entity).to[List[DisplayIngestMinimal]]
                whenReady(
                  displayBagIngestFutures
                ) { displayBagIngests =>
                  displayBagIngests shouldBe List.empty

                  assertMetricSent(
                    metricsSender,
                    result = HttpMetricResults.UserError
                  )
                }
            }
          }
      }
    }
  }
}
