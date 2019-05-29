package uk.ac.wellcome.platform.archive.bagverifier.models

import java.time.{Duration, Instant}

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.verify.VerificationSuccess
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class VerificationSummaryTest
    extends FunSpec
    with Matchers
    with ObjectLocationGenerators {

  it("calculates duration once completed") {
    val location = createObjectLocation
    val time = Instant.now.minus(Duration.ofSeconds(1))
    val summary = VerificationSuccess(Nil)

    val result = VerificationSummary.create(location, summary, time)

    result.duration shouldBe defined
    result.duration.get.toMillis shouldBe >(0l)
  }

  it("calculates duration as None when verification is not completed") {
    val location = createObjectLocation
    val time = Instant.now.minus(Duration.ofSeconds(1))
    val e = new RuntimeException("BOOM!")

    val result = VerificationSummary.incomplete(location, e, time)

    result.duration shouldBe None
  }
}
