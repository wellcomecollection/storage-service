package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.DoesNotExistError

trait SizeFinderTestCases[Ident, Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withSizeFinder[R](testWith: TestWith[SizeFinder[Ident], R])(
    implicit context: Context
  ): R

  def createIdent(implicit context: Context): Ident

  def createObject(ident: Ident, contents: String)(
    implicit context: Context
  ): Unit

  describe("it behaves as a size finder") {
    it("finds the sizes of an object") {
      withContext { implicit context =>
        val ident = createIdent
        createObject(ident, "the quick brown fox")

        val result = withSizeFinder {
          _.getSize(ident)
        }

        result.right.value shouldBe 19
      }
    }

    it("returns a DoesNotExistError if the object doesn't exist") {
      withContext { implicit context =>
        val ident = createIdent

        val result = withSizeFinder {
          _.getSize(ident)
        }

        result.left.value shouldBe a[DoesNotExistError]
      }
    }
  }
}
