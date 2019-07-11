package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model._
import io.circe.optics.JsonPath._
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  BagInfoGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.display.fixtures.DisplayJsonHelpers
import uk.ac.wellcome.platform.storage.bags.api.fixtures.BagsApiFixture

class BagsApiFeatureTest
    extends FunSpec
    with Matchers
    with BagIdGenerators
    with BagInfoGenerators
    with BagsApiFixture
    with StorageManifestGenerators
    with DisplayJsonHelpers
    with JsonAssertions
    with IntegrationPatience {

  describe("GET /bags/:space/:id") {
    it("finds the latest version of a bag") {
      val storageManifest = createStorageManifestWith(
        locations = List(
          createObjectLocation,
          createObjectLocation,
          createObjectLocation
        )
      )

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          val expectedJson =
            s"""
              |{
              |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
              |  "id": "${storageManifest.id.toString}",
              |  "space": {
              |    "id": "${storageManifest.space.underlying}",
              |    "type": "Space"
              |  },
              |  "info": ${bagInfo(storageManifest.info)},
              |  "manifest": ${manifest(storageManifest.manifest)},
              |  "tagManifest": ${manifest(storageManifest.tagManifest)},
              |  "locations": [
              |    ${asList(storageManifest.locations, location)}
              |  ],
              |  "createdDate": "${DateTimeFormatter.ISO_INSTANT.format(
                 storageManifest.createdDate)}",
              |  "version": "v${storageManifest.version}",
              |  "type": "Bag"
              |}
              """.stripMargin

          val url =
            s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) { actualJson =>
              assertJsonStringsAreEqual(actualJson, expectedJson)
            }

            assertMetricSent(
              metrics,
              result = HttpMetricResults.Success
            )
          }
      }
    }

    it("finds a specific version of a bag") {
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          bagInfo = createBagInfoWith(
            externalIdentifier = externalIdentifier
          ),
          space = storageSpace,
          version = version
        )
      }

      withConfiguredApp(initialManifests = manifests) {
        case (_, metrics, baseUrl) =>
          manifests.foreach { storageManifest =>
            val expectedJson =
              s"""
                |{
                |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
                |  "id": "${storageManifest.id.toString}",
                |  "space": {
                |    "id": "${storageManifest.space.underlying}",
                |    "type": "Space"
                |  },
                |  "info": ${bagInfo(storageManifest.info)},
                |  "manifest": ${manifest(storageManifest.manifest)},
                |  "tagManifest": ${manifest(storageManifest.tagManifest)},
                |  "locations": [
                |    ${asList(storageManifest.locations, location)}
                |  ],
                |  "createdDate": "${DateTimeFormatter.ISO_INSTANT.format(
                   storageManifest.createdDate)}",
                |  "version": "v${storageManifest.version}",
                |  "type": "Bag"
                |}
                """.stripMargin

            val url =
              s"$baseUrl/bags/${storageSpace.underlying}/${externalIdentifier.underlying}?version=v${storageManifest.version}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) { actualJson =>
                assertJsonStringsAreEqual(actualJson, expectedJson)
              }

              assertMetricSent(
                metrics,
                result = HttpMetricResults.Success
              )
            }
          }
      }
    }

    it("does not output null values") {
      val storageManifest = createStorageManifestWith(
        bagInfo = createBagInfoWith(externalDescription = None)
      )

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, _, baseUrl) =>
          whenGetRequestReady(
            s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}") {
            response =>
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) { jsonString =>
                val infoJson =
                  root.info.json
                    .getOption(parse(jsonString).right.get)
                    .get
                infoJson.findAllByKey("externalDescription") shouldBe empty
              }
          }
      }
    }

    it("returns a 404 NotFound if there are no manifests for this bag ID") {
      withConfiguredApp() {
        case (_, metrics, baseUrl) =>
          val bagId = createBagId
          whenGetRequestReady(
            s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}") {
            response =>
              assertIsUserErrorResponse(
                response,
                description = s"Storage manifest $bagId not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("returns a 404 NotFound if you ask for a bag ID in the wrong space") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          val badId =
            s"${storageManifest.space}123/${storageManifest.id.externalIdentifier}"
          whenGetRequestReady(s"$baseUrl/bags/$badId") { response =>
            assertIsUserErrorResponse(
              response,
              description = s"Storage manifest $badId not found",
              statusCode = StatusCodes.NotFound,
              label = "Not Found"
            )

            assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("returns a 404 NotFound if you ask for a version that doesn't exist") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(
            s"$baseUrl/bags/${storageManifest.space}/${storageManifest.id.externalIdentifier}?version=v${storageManifest.version + 1}") {
            response =>
              assertIsUserErrorResponse(
                response,
                description =
                  s"Storage manifest ${storageManifest.id} version v${storageManifest.version + 1} not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("returns a 404 NotFound if you ask for a non-numeric version") {
      val badVersion = randomAlphanumeric

      val bagId = createBagId

      withConfiguredApp() {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$bagId?version=$badVersion") {
            response =>
              assertIsUserErrorResponse(
                response,
                description =
                  s"Storage manifest $bagId version $badVersion not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    // TODO: Come back and restore this test when we can reliably
    // break the underlying tracker.
    ignore("returns a 500 error if looking up the bag fails") {
      withBrokenApp {
        case (_, metrics, baseUrl) =>
          val bagId = createBagId
          whenGetRequestReady(
            s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}") {
            response =>
              assertIsInternalServerErrorResponse(response)

              assertMetricSent(metrics, result = HttpMetricResults.ServerError)
          }
      }
    }
  }
}
