package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.DoesNotExistError
import uk.ac.wellcome.storage.streaming.Codec

trait LargeStreamReaderTestCases[Ident, Namespace] extends AnyFunSpec with Matchers with EitherValues with StorageRandomThings {
  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def createIdentWith(namespace: Namespace): Ident

  def writeString(ident: Ident, contents: String): Unit

  def withLargeStreamReader[R](bufferSize: Long)(testWith: TestWith[LargeStreamReader[Ident], R]): R

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
}
