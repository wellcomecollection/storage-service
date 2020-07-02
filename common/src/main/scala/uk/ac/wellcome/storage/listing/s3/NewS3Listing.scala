package uk.ac.wellcome.storage.listing.s3

import uk.ac.wellcome.storage.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.listing.Listing

trait NewS3Listing[ListingResult] extends Listing[S3ObjectLocationPrefix, ListingResult]
