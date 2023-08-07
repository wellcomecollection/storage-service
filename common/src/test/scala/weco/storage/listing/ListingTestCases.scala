package weco.storage.listing

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import weco.fixtures.RandomGenerators
import weco.storage.listing.fixtures.ListingFixtures

trait ListingTestCases[Ident,
                       Prefix,
                       ListingResult,
                       ListingImpl <: Listing[Prefix, ListingResult],
                       ListingContext]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with RandomGenerators
    with ListingFixtures[
      Ident,
      Prefix,
      ListingResult,
      ListingImpl,
      ListingContext] {
  def createIdent(implicit context: ListingContext): Ident
  def extendIdent(id: Ident, extension: String): Ident
  def createPrefix: Prefix
  def createPrefixMatching(id: Ident): Prefix

  def assertResultCorrect(result: Iterable[ListingResult],
                          entries: Seq[Ident])(implicit context: ListingContext): Assertion

  describe("behaves as a Listing") {
    describe("list") {
      it("doesn't find anything in an empty store") {
        withListingContext { implicit context =>
          val ident = createIdent
          val prefix = createPrefixMatching(ident)

          withListing(context, initialEntries = Seq.empty) { listing =>
            listing.list(prefix).value shouldBe empty
          }
        }
      }

      it("finds a single entry where the prefix matches the ident") {
        withListingContext { implicit context =>
          val ident = createIdent
          val prefix = createPrefixMatching(ident)
          val entries = Seq(ident)

          withListing(context, initialEntries = entries) { listing =>
            assertResultCorrect(
              result = listing.list(prefix).value,
              entries = entries
            )
          }
        }
      }

      it("finds a single entry where the prefix is strictly shorter than the ident") {
        withListingContext { implicit context =>
          val ident = createIdent
          val prefix = createPrefixMatching(ident)
          val entries = Seq(extendIdent(ident, randomAlphanumeric()))

          withListing(context, initialEntries = entries) { listing =>
            assertResultCorrect(
              result = listing.list(prefix).value,
              entries = entries
            )
          }
        }
      }

      it("finds multiple matching entries") {
        withListingContext { implicit context =>
          val ident = createIdent
          val entries = Seq("1.txt", "2.txt", "3.txt").map { filename =>
            extendIdent(ident, filename)
          }
          val prefix = createPrefixMatching(ident)

          withListing(context, initialEntries = entries) { listing =>
            assertResultCorrect(
              result = listing.list(prefix).value,
              entries = entries
            )
          }
        }
      }

      it("ignores entries that don't match") {
        withListingContext { implicit context =>
          val ident = createIdent
          val entries = Seq("1.txt", "2.txt", "3.txt").map { filename =>
            extendIdent(ident, filename)
          }
          val prefix = createPrefixMatching(ident)

          val extraEntries = Seq(createIdent, createIdent)

          withListing(context, initialEntries = entries ++ extraEntries) {
            listing =>
              assertResultCorrect(
                result = listing.list(prefix).value,
                entries = entries
              )
          }
        }
      }
    }
  }
}
