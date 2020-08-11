package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import java.net.URI

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.azure.AzureSizeFinder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.azure.AzureBlobMetadata

class AzureFixityChecker(implicit blobClient: BlobServiceClient)
    extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {
  override protected val streamStore: StreamStore[AzureBlobLocation] =
    new AzureStreamStore()
  override protected val sizeFinder: SizeFinder[AzureBlobLocation] =
    new AzureSizeFinder()
  override val tags: Tags[AzureBlobLocation] = new AzureBlobMetadata()
  override implicit val locator
    : Locatable[AzureBlobLocation, AzureBlobLocationPrefix, URI] =
    new AzureLocatable
}
