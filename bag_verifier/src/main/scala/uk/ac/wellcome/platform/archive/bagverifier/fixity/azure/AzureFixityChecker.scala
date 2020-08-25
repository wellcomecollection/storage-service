package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.azure.{
  AzureLargeStreamReader,
  AzureSizeFinder
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureFixityChecker(implicit blobClient: BlobServiceClient)
    extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {

  // The verifier gets 4GB of memory, and may be processing up to ten objects at once.
  // Allowing a GB of overhead for other functions, let it read 200MB at a time.
  //
  // This means reading a 166GB object (the largest so far in the storage service) will
  // require 830 Read calls.
  //
  // In practice most objects are well under this threshhold, so it won't be maxing out.
  override protected val streamReader
    : Readable[AzureBlobLocation, InputStreamWithLength] =
    new AzureLargeStreamReader(bufferSize = 200 * 1000 * 1000)

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
