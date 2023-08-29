package weco.storage.listing.fixtures

import weco.fixtures.TestWith
import weco.storage.listing.Listing

trait ListingFixtures[Ident,
                      Prefix,
                      ListingResult,
                      ListingImpl <: Listing[Prefix, ListingResult],
                      ListingContext]
    extends {
  def withListingContext[R](testWith: TestWith[ListingContext, R]): R

  def withListing[R](context: ListingContext, initialEntries: Seq[Ident])(
    testWith: TestWith[ListingImpl, R]): R
}
