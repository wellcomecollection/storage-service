package uk.ac.wellcome.platform.archive.bagreplicator.storage.azure

import com.azure.storage.blob.models.BlobRange
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BlobRangeUtilTest extends AnyFunSpec with Matchers {
  describe("getRanges") {
    it("works if length is a multiple of blockSize") {
      val length = 15
      val blockSize = 5

      val actualRanges =
        BlobRangeUtil
          .getRanges(length = length, blockSize = blockSize)

      val expectedRanges = Seq(
        new BlobRange(0L, 5L),
        new BlobRange(5L, 5L),
        new BlobRange(10L, 5L)
      )

      assertEqual(actualRanges, expectedRanges)
    }

    it("works if length is not a multiple of blockSize") {
      val length = 17
      val blockSize = 5

      val actualRanges =
        BlobRangeUtil
          .getRanges(length = length, blockSize = blockSize)

      val expectedRanges = Seq(
        new BlobRange(0L, 5L),
        new BlobRange(5L, 5L),
        new BlobRange(10L, 5L),
        new BlobRange(15L),
      )

      assertEqual(actualRanges, expectedRanges)
    }
  }

  private def assertEqual(seq1: Seq[BlobRange], seq2: Seq[BlobRange]): Seq[Assertion] = {
    seq1.zip(seq2).map { case (b1, b2) =>
      b1.getCount shouldBe b2.getCount
      b1.getOffset shouldBe b2.getOffset
    }
  }

  describe("getBlockIdentifiers") {
    it("creates a single identifier") {
      BlobRangeUtil.getBlockIdentifiers(count = 1) shouldBe Seq("MQ==")
    }

    it("always creates identifiers of the same length") {
      BlobRangeUtil.getBlockIdentifiers(count = 100)
        .map { _.length }
        .toSet shouldBe Set(4)
    }
  }
}
