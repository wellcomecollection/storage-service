package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  BagInfoGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.fixtures.DisplayJsonHelpers
import uk.ac.wellcome.platform.storage.bags.api.fixtures.BagsApiFixture
import uk.ac.wellcome.storage.fixtures.S3Fixtures

/** Tests for GET /bags/:space/:id/versions
  *
  */
class LookupBagVersionsApiTest
  extends FunSpec
    with Matchers
    with BagIdGenerators
    with BagInfoGenerators
    with BagsApiFixture
    with StorageManifestGenerators
    with DisplayJsonHelpers
    with JsonAssertions
    with S3Fixtures
    with IntegrationPatience {

  it("returns a 404 NotFound if there are no manifests for this bag ID") {
    withConfiguredApp() {
      case (_, metrics, baseUrl) =>
        val bagId = createBagId
        whenGetRequestReady(
          s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}/versions"
        ) { response =>
          assertIsUserErrorResponse(
            response,
            description = s"No storage manifest versions found for $bagId",
            statusCode = StatusCodes.NotFound,
            label = "Not Found"
          )

          assertMetricSent(metrics, result = HttpMetricResults.UserError)
        }
    }
  }

  it("finds a single version of a storage manifest") {
    val storageManifest = createStorageManifest

    val initialManifests = Seq(storageManifest)

    withConfiguredApp(initialManifests) {
      case (_, metrics, baseUrl) =>
        val expectedJson =
          s"""
             |{
             |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
             |  "type": "ResultList",
             |  "results": [${expectedVersionJson(storageManifest)}]
             |}
            """.stripMargin

        val url =
          s"$baseUrl/bags/${storageManifest.id.space}/${storageManifest.id.externalIdentifier}/versions"

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

  it("finds multiple versions of a manifest") {
    val storageManifest = createStorageManifest

    val multipleManifests = (1 to 5).map { version =>
      version -> storageManifest.copy(
        createdDate = randomInstant,
        version = BagVersion(version)
      )
    }.toMap

    val initialManifests = multipleManifests.values.toSeq.sortBy {
      _.version.underlying
    }

    withConfiguredApp(initialManifests) {
      case (_, metrics, baseUrl) =>
        val expectedJson =
          s"""
             |{
             |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
             |  "type": "ResultList",
             |  "results": [
             |    ${expectedVersionJson(
            multipleManifests(5),
            expectedVersion = "v5"
          )},
             |    ${expectedVersionJson(
            multipleManifests(4),
            expectedVersion = "v4"
          )},
             |    ${expectedVersionJson(
            multipleManifests(3),
            expectedVersion = "v3"
          )},
             |    ${expectedVersionJson(
            multipleManifests(2),
            expectedVersion = "v2"
          )},
             |    ${expectedVersionJson(
            multipleManifests(1),
            expectedVersion = "v1"
          )}
             |  ]
             |}
            """.stripMargin

        val url =
          s"$baseUrl/bags/${storageManifest.id.space}/${storageManifest.id.externalIdentifier}/versions"

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

  it("supports searching for manifests before a given version") {
    val storageManifest = createStorageManifest

    val multipleManifests = (1 to 5).map { version =>
      version -> storageManifest.copy(
        createdDate = randomInstant,
        version = BagVersion(version)
      )
    }.toMap

    val initialManifests = multipleManifests.values.toSeq.sortBy {
      _.version
    }

    withConfiguredApp(initialManifests) {
      case (_, metrics, baseUrl) =>
        val expectedJson =
          expectedVersionList(
            expectedVersionJson(multipleManifests(3), expectedVersion = "v3"),
            expectedVersionJson(multipleManifests(2), expectedVersion = "v2"),
            expectedVersionJson(multipleManifests(1), expectedVersion = "v1")
          )

        val url =
          s"$baseUrl/bags/${storageManifest.id.space}/${storageManifest.id.externalIdentifier}/versions?before=v4"

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

  it(
    "returns a 404 NotFound if there are no manifests before the specified version"
  ) {
    val storageManifest = createStorageManifest

    val multipleManifests = (5 to 10).map { version =>
      version -> storageManifest.copy(
        createdDate = randomInstant,
        version = BagVersion(version)
      )
    }.toMap

    val initialManifests = multipleManifests.values.toSeq.sortBy {
      _.version
    }

    withConfiguredApp(initialManifests) {
      case (_, metrics, baseUrl) =>
        whenGetRequestReady(
          s"$baseUrl/bags/${storageManifest.id}/versions?before=v4"
        ) { response =>
          assertIsUserErrorResponse(
            response,
            description =
              s"No storage manifest versions found for ${storageManifest.id} before v4",
            statusCode = StatusCodes.NotFound,
            label = "Not Found"
          )

          assertMetricSent(metrics, result = HttpMetricResults.UserError)
        }
    }
  }

  it(
    "returns a 400 UserError if search for manifests before a non-numeric version"
  ) {
    val badBefore = randomAlphanumeric

    withConfiguredApp() {
      case (_, metrics, baseUrl) =>
        whenGetRequestReady(
          s"$baseUrl/bags/$createBagId/versions?before=$badBefore"
        ) { response =>
          assertIsUserErrorResponse(
            response,
            description = s"Cannot parse version string: $badBefore",
            statusCode = StatusCodes.BadRequest
          )

          assertMetricSent(metrics, result = HttpMetricResults.UserError)
        }
    }
  }

  it("returns a 500 if looking up the lists of versions fails") {
    withBrokenApp {
      case (metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/bags/$createBagId/versions") {
          response =>
            assertIsInternalServerErrorResponse(response)

            assertMetricSent(metrics, result = HttpMetricResults.ServerError)
        }
    }
  }

  private def expectedVersionJson(
    storageManifest: StorageManifest,
    expectedVersion: String
  ): String =
    expectedVersionJson(
      storageManifest,
      expectedVersion = Some(expectedVersion)
    )

  private def expectedVersionList(results: String*): String =
    s"""
       |{
       |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
       |  "type": "ResultList",
       |  "results": [${results.mkString(", ")}]
       |}""".stripMargin

  private def expectedVersionJson(
    storageManifest: StorageManifest,
    expectedVersion: Option[String] = None
  ): String = {
    val createdDate =
      DateTimeFormatter.ISO_INSTANT.format(storageManifest.createdDate)

    s"""
       |{
       |  "id": "${storageManifest.id.toString}",
       |  "version": "${expectedVersion.getOrElse(storageManifest.version)}",
       |  "createdDate": "$createdDate",
       |  "type": "Bag"
       |}
       """.stripMargin
  }
}
