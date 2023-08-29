package weco.storage.listing.s3

import weco.storage.listing.Listing
import weco.storage.providers.s3.S3ObjectLocationPrefix

trait S3Listing[ListingResult]
    extends Listing[S3ObjectLocationPrefix, ListingResult]
