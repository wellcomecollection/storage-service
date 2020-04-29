package uk.ac.wellcome.platform.storage.ingests_tracker

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.fixtures.{HttpFixtures, StorageRandomThings}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture

class IngestsTrackerApiFeatureTest
  extends FunSpec
    with Matchers
    with IngestsTrackerApiFixture
    with JsonAssertions
    with HttpFixtures
    with StorageRandomThings {

  it("starts") {
    withConfiguredApp() { _ =>
      val path = s"/ingests/id"

      whenGetRequestReady(path) { result =>
        result.status shouldBe StatusCodes.NotFound
      }
    }
  }
}
