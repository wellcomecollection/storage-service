package weco.storage_service.notifier.services

import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.pekko.fixtures.Pekko
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.ingests.models.{Callback, IngestID}
import weco.fixtures.TimeAssertions

import scala.util.{Failure, Success, Try}

class PrepareNotificationServiceTest
    extends AnyFunSpec
    with Matchers
    with Pekko
    with TimeAssertions
    with StorageRandomGenerators {

  val id: IngestID = createIngestID

  val successfulStatuscodes =
    Table(
      "success status code",
      200,
      201,
      202,
      203,
      204,
      205,
      206,
      207,
      208,
      226
    )
  it("returns a successful IngestUpdate when a callback succeeds") {
    forAll(successfulStatuscodes) { responseStatus: Int =>
      assertPreparedNotificationIsCorrect(
        httpResponse = Success(HttpResponse(status = responseStatus)),
        expectedCallbackStatus = Callback.Succeeded,
        expectedDescription = "Callback fulfilled"
      )
    }
  }

  val failedStatusCodes =
    Table(
      ("failed status code", "msg"),
      (500, s"Callback failed for: $id, got 500 Internal Server Error!"),
      (400, s"Callback failed for: $id, got 400 Bad Request!")
    )
  it(
    "returns a failed IngestUpdate when a callback returns with a failed status code"
  ) {
    forAll(failedStatusCodes) {
      (responseStatus: Int, expectedDescription: String) =>
        assertPreparedNotificationIsCorrect(
          httpResponse = Success(HttpResponse(responseStatus)),
          expectedCallbackStatus = Callback.Failed,
          expectedDescription = expectedDescription
        )
    }
  }

  it("returns a failed IngestUpdate when a callback fails") {
    val exception = new RuntimeException("Callback exception!")
    assertPreparedNotificationIsCorrect(
      httpResponse = Failure(exception),
      expectedCallbackStatus = Callback.Failed,
      expectedDescription =
        s"Callback failed for: $id (${exception.getMessage})"
    )
  }

  private def assertPreparedNotificationIsCorrect(
    httpResponse: Try[HttpResponse],
    expectedCallbackStatus: Callback.CallbackStatus,
    expectedDescription: String
  ): Assertion = {
    val result = PrepareNotificationService.prepare(
      id = id,
      httpResponse = httpResponse
    )

    result.id shouldBe id
    result.callbackStatus shouldBe expectedCallbackStatus

    result.events should have size 1
    val event = result.events.head
    event.description shouldBe expectedDescription
    assertRecent(event.createdDate)
  }
}
