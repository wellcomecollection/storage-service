package weco.storage.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.{RandomGenerators, TestWith}
import weco.storage.models.ByteRange
import weco.storage.streaming.Codec
import weco.storage.{
  DoesNotExistError,
  ReadError,
  RetryableError,
  StoreReadError
}

trait LargeStreamReaderTestCases[Ident, Namespace]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with RandomGenerators {
  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def createIdentWith(namespace: Namespace): Ident

  def writeString(ident: Ident, contents: String): Unit

  def withRangedReader[R](testWith: TestWith[RangedReader[Ident], R]): R

  def withLargeStreamReader[R](
    bufferSize: Long,
    rangedReader: RangedReader[Ident]
  )(testWith: TestWith[LargeStreamReader[Ident], R]): R

  def withLargeStreamReader[R](
    bufferSize: Long
  )(testWith: TestWith[LargeStreamReader[Ident], R]): R =
    withRangedReader { rangedReader =>
      withLargeStreamReader(bufferSize, rangedReader = rangedReader) {
        streamReader =>
          testWith(streamReader)
      }
    }

  it("combines multiple streams together") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      val bufferSize = 500
      val contents = randomAlphanumeric(length = bufferSize * 3)

      writeString(ident, contents)

      withLargeStreamReader(bufferSize) { reader =>
        val result = reader.get(ident).value
        val retrievedContents =
          Codec.stringCodec.fromStream(result.identifiedT).value

        retrievedContents shouldBe contents
      }
    }
  }

  it(
    "combines multiple streams when total size is not a multiple of buffer size"
  ) {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      val bufferSize = 500
      val contents = randomAlphanumeric(length = bufferSize * 3 + 1)

      writeString(ident, contents)

      withLargeStreamReader(bufferSize) { reader =>
        val result = reader.get(ident).value
        val retrievedContents =
          Codec.stringCodec.fromStream(result.identifiedT).value

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

      val contents = randomAlphanumeric()
      writeString(ident, contents)

      var rangedReaderCalls = 0

      val brokenReader = new RangedReader[Ident] {
        override def getBytes(
          id: Ident,
          range: ByteRange
        ): Either[ReadError, Array[Byte]] = {
          rangedReaderCalls += 1
          Left(new StoreReadError(new Throwable("BOOM!")) with RetryableError)
        }
      }

      intercept[LargeStreamReaderCannotReadRange[Ident]] {
        withLargeStreamReader(bufferSize = 500, rangedReader = brokenReader) {
          _.get(ident)
        }
      }

      rangedReaderCalls shouldBe 3
    }
  }
}
