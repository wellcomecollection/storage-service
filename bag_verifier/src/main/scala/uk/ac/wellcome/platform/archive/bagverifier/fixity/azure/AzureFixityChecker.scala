package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.azure.AzureSizeFinder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store.azure.AzureStreamStore

class AzureFixityChecker(implicit blobClient: BlobServiceClient)
    extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {
  override protected val streamStore =
    new AzureStreamStore()

  override protected val sizeFinder =
    new AzureSizeFinder()

  /**
    * The FixityChecker tags objects with their checksum so that,
    * if they are lifecycled to cold storage, we don't need to read them to know the checksum.
    * This is useful for files referenced in the fetch file. However, references in the fetch file
    * will always point to the primary bucket in S3, never Azure, so there's no need to tag in the AzureFixityChecker
    */
  override val tags = None

  override implicit val locator: AzureLocatable = new AzureLocatable
}
