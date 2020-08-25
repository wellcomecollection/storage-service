package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.storage.models.{
  ClosedByteRange,
  OpenByteRange
}

class ByteRangeUtilTest extends AnyFunSpec with Matchers {
  describe("partition") {
    it("works if length is a multiple of bufferSize") {
      val length = 15
      val bufferSize = 5

      val actualRanges =
        ByteRangeUtil.partition(length = length, bufferSize = bufferSize)

      val expectedRanges = Seq(
        ClosedByteRange(0L, 5L),
        ClosedByteRange(5L, 5L),
        ClosedByteRange(10L, 5L)
      )

      actualRanges shouldBe expectedRanges
    }

    it("works if length is not a multiple of bufferSize") {
      val length = 17
      val bufferSize = 5

      val actualRanges =
        ByteRangeUtil.partition(length = length, bufferSize = bufferSize)

      val expectedRanges = Seq(
        ClosedByteRange(0L, 5L),
        ClosedByteRange(5L, 5L),
        ClosedByteRange(10L, 5L),
        OpenByteRange(15L)
      )

      actualRanges shouldBe expectedRanges
    }
  }
}
