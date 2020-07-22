package uk.ac.wellcome.storage.transfer.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.storage.store.s3.{S3StreamReadable, S3StreamStore}
import uk.ac.wellcome.storage.transfer._
import uk.ac.wellcome.storage.{AzureBlobItemLocation, S3ObjectLocation}

class NewS3toAzureTransfer(implicit blobClient: BlobServiceClient, s3Client: AmazonS3)
  extends NewTransfer[S3ObjectLocation, AzureBlobItemLocation] {

  implicit val s3Readable: S3StreamReadable = new S3StreamStore()

  override protected val underlying = new S3toAzureTransfer()
}
