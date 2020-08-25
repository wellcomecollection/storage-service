package uk.ac.wellcome.platform.archive.common.storage.models

import com.azure.storage.blob.models.BlobRange
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ByteRangeTest extends AnyFunSpec with Matchers {
  it("can be constructed from a BlobRange") {
    val b1 = new BlobRange(10)
    ByteRange(b1) shouldBe OpenByteRange(10)

    val b2 = new BlobRange(10L, 5L)
    ByteRange(b2) shouldBe ClosedByteRange(10, 5)
  }
}
