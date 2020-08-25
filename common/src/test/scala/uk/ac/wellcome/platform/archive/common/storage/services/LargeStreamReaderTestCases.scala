package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.models.ByteRange
import uk.ac.wellcome.storage.{DoesNotExistError, ReadError, RetryableError, StoreReadError}
import uk.ac.wellcome.storage.streaming.Codec

trait LargeStreamReaderTestCases[Ident, Namespace] extends AnyFunSpec with Matchers with EitherValues with StorageRandomThings {
  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def createIdentWith(namespace: Namespace): Ident

  def writeString(ident: Ident, contents: String): Unit

  def withRangedReader[R](testWith: TestWith[RangedReader[Ident], R]): R

  def withLargeStreamReader[R](bufferSize: Long, rangedReader: RangedReader[Ident])(testWith: TestWith[LargeStreamReader[Ident], R]): R

  def withLargeStreamReader[R](bufferSize: Long)(testWith: TestWith[LargeStreamReader[Ident], R]): R =
    withRangedReader { rangedReader =>
      withLargeStreamReader(bufferSize, rangedReader = rangedReader) { streamReader =>
        testWith(streamReader)
      }
    }

  it("combines multiple streams together") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      val bufferSize = 500
      val contents = randomAlphanumericWithLength(length = bufferSize * 3)

      writeString(ident, contents)

      withLargeStreamReader(bufferSize) { reader =>
        val result = reader.get(ident).right.value
        val retrievedContents = Codec.stringCodec.fromStream(result.identifiedT).right.value

        retrievedContents shouldBe contents
      }
    }
  }

  it("returns a DoesNotExistError for a missing object") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      withLargeStreamReader(bufferSize = 500) {
        _.get(ident).left.value shouldBe a[DoesNotExistError]
      }
    }
  }

  it("retries a RangedReader call up to three times") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      val contents = randomAlphanumeric
      writeString(ident, contents)

      var rangedReaderCalls = 0

      val brokenReader = new RangedReader[Ident] {
        override def getBytes(id: Ident, range: ByteRange): Either[ReadError, Array[Byte]] = {
          rangedReaderCalls += 1
          Left(new StoreReadError(new Throwable("BOOM!")) with RetryableError)
        }
      }

      intercept[RuntimeException] {
        withLargeStreamReader(bufferSize = 500, rangedReader = brokenReader) {
          _.get(ident)
        }
      }

      rangedReaderCalls shouldBe 3
    }
  }
}
