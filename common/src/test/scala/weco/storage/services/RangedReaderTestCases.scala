package weco.storage.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage.DoesNotExistError
import weco.storage.models.{ClosedByteRange, OpenByteRange}

trait RangedReaderTestCases[Ident, Namespace]
    extends AnyFunSpec
    with Matchers
    with EitherValues {
  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def createIdentWith(namespace: Namespace): Ident

  def writeString(ident: Ident, contents: String): Unit

  def withRangedReader[R](testWith: TestWith[RangedReader[Ident], R]): R

  it("reads part of an object") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      writeString(ident, contents = "Hello world")

      val receivedBytes = withRangedReader {
        _.getBytes(ident, range = ClosedByteRange(start = 1, count = 4))
      }

      receivedBytes.value shouldBe "ello".getBytes()
    }
  }

  it("reads an open range to the end of an object") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      writeString(ident, contents = "Hello world")

      val receivedBytes = withRangedReader {
        _.getBytes(ident, range = OpenByteRange(start = 6))
      }

      receivedBytes.value shouldBe "world".getBytes()
    }
  }

  it("reads a closed range that skips past the end of an object") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      writeString(ident, contents = "Hello world")

      val receivedBytes = withRangedReader {
        _.getBytes(ident, range = ClosedByteRange(start = 6, count = 50))
      }

      receivedBytes.value shouldBe "world".getBytes()
    }
  }

  it("returns a DoesNotExistError for a non-existent object") {
    withNamespace { namespace =>
      val ident = createIdentWith(namespace)

      val receivedBytes = withRangedReader {
        _.getBytes(ident, range = ClosedByteRange(start = 6, count = 50))
      }

      receivedBytes.left.value shouldBe a[DoesNotExistError]
    }
  }
}
