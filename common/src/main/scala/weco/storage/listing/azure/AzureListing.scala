package weco.storage.listing.azure

import weco.storage.listing.Listing
import weco.storage.listing.Listing
import weco.storage.providers.azure.AzureBlobLocationPrefix

trait AzureListing[Result] extends Listing[AzureBlobLocationPrefix, Result]
