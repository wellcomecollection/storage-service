package uk.ac.wellcome.platform.archive.bags.async.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bags.async.fixtures.WorkerServiceFixture

class BagsWorkerServiceTest extends FunSpec with Matchers with WorkerServiceFixture {
  it("obeys truth") {
    true shouldBe true
  }
}
