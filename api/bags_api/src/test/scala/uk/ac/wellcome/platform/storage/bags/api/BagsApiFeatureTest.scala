package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ETag, Location}
import io.circe.optics.JsonPath._
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  BagInfoGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.fixtures.DisplayJsonHelpers
import uk.ac.wellcome.platform.storage.bags.api.fixtures.BagsApiFixture
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class BagsApiFeatureTest
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

  describe("GET /bags/:space/:id") {
    it("finds the latest version of a bag") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          val url =
            s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifest)
            }

            val header: ETag = response.header[ETag].get
            val etagValue = header.etag.value.replace("\"", "")

            etagValue shouldBe storageManifest.idWithVersion

            assertMetricSent(
              metrics,
              result = HttpMetricResults.Success
            )
          }
      }
    }

    it("can return very large responses") {
      // This is not an exact mechanism!
      // We can experiment to identify a size which exceeds the maxResponseByteLength
      val storageManifest = createStorageManifestWithFileCount(fileCount = 100)

      withLocalS3Bucket { bucket =>
        val prefix = ObjectLocationPrefix(bucket.name, randomAlphanumeric)

        withConfiguredApp(
          initialManifests = Seq(storageManifest),
          locationPrefix = prefix,
          maxResponseByteLength = 1000
        ) {
          case (_, metrics, baseUrl) =>
            val url =
              s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.TemporaryRedirect

              assertMetricSent(
                metrics,
                result = HttpMetricResults.Success
              )

              val redirectedUrl = response.header[Location].get.uri.toString()

              whenGetRequestReady(redirectedUrl) { redirectedResponse =>
                withStringEntity(redirectedResponse.entity) {
                  assertJsonMatches(_, storageManifest)
                }
              }
            }

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
          version = BagVersion(version)
        )
      }

      withConfiguredApp(initialManifests = manifests) {
        case (_, metrics, baseUrl) =>
          manifests.foreach { storageManifest =>
            val url =
              s"$baseUrl/bags/${storageSpace.underlying}/${externalIdentifier.underlying}?version=${storageManifest.version}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) {
                assertJsonMatches(_, storageManifest)
              }

              val header: ETag = response.header[ETag].get
              val etagValue = header.etag.value.replace("\"", "")

              etagValue shouldBe s"${storageManifest.space}/${storageManifest.info.externalIdentifier}/${storageManifest.version}"

              assertMetricSent(
                metrics,
                result = HttpMetricResults.Success
              )
            }
          }
      }
    }

    val storageManifestWithSlash = createStorageManifestWith(
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("alfa/bravo")
      )
    )

    it("finds a bag with a slash in the external identifier (URL-encoded)") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa%2Fbravo"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifestWithSlash)
            }
          }
      }
    }

    it("finds a bag with a slash in the external identifier (not URL-encoded)") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa/bravo"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifestWithSlash)
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
            s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"
          ) { response =>
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

    describe("returns a 404 Not Found for missing bags") {
      it("if there are no manifests for this bag ID") {
        withConfiguredApp() {
          case (_, metrics, baseUrl) =>
            val bagId = createBagId
            whenGetRequestReady(
              s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}"
            ) { response =>
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

      it("if you ask for a bag ID in the wrong space") {
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

      it("if you ask for a version that doesn't exist") {
        val storageManifest = createStorageManifest

        withConfiguredApp(initialManifests = Seq(storageManifest)) {
          case (_, metrics, baseUrl) =>
            whenGetRequestReady(
              s"$baseUrl/bags/${storageManifest.space}/${storageManifest.id.externalIdentifier}?version=${storageManifest.version.increment}"
            ) { response =>
              assertIsUserErrorResponse(
                response,
                description =
                  s"Storage manifest ${storageManifest.id} version ${storageManifest.version.increment} not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(metrics, result = HttpMetricResults.UserError)
            }
        }
      }

      it("if you ask for a non-numeric version") {
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
    }

    it("returns a 500 error if looking up the bag fails") {
      withBrokenApp {
        case (metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$createBagId") { response =>
            assertIsInternalServerErrorResponse(response)

            assertMetricSent(metrics, result = HttpMetricResults.ServerError)
          }
      }
    }

    it("returns a 500 error if looking up a specific version of a bag fails") {
      withBrokenApp {
        case (metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$createBagId?version=v1") {
            response =>
              assertIsInternalServerErrorResponse(response)

              assertMetricSent(metrics, result = HttpMetricResults.ServerError)
          }
      }
    }
  }

  describe("GET /bags/:space/:id/versions") {
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

    val storageManifestWithSlash = createStorageManifestWith(
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("alfa/bravo")
      )
    )

    it("finds versions of a bag with a slash in the external identifier (URL-encoded)") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa%2Fbravo/versions"

          val expectedJson = expectedVersionList(
            expectedVersionJson(storageManifestWithSlash)
          )

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonStringsAreEqual(_, expectedJson)
            }
          }
      }
    }

    it("finds versions of a bag with a slash in the external identifier (not URL-encoded)") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa/bravo/versions"

          val expectedJson = expectedVersionList(
            expectedVersionJson(storageManifestWithSlash)
          )

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonStringsAreEqual(_, expectedJson)
            }
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

  private def assertJsonMatches(
    json: String,
    storageManifest: StorageManifest
  ): Assertion = {
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
         |  "location": ${location(storageManifest.location)},
         |  "replicaLocations": [
         |    ${asList(storageManifest.replicaLocations, location)}
         |  ],
         |  "createdDate": "${DateTimeFormatter.ISO_INSTANT.format(
           storageManifest.createdDate
         )}",
         |  "version": "${storageManifest.version}",
         |  "type": "Bag"
         |}
       """.stripMargin

    assertJsonStringsAreEqual(json, expectedJson)
  }
}
