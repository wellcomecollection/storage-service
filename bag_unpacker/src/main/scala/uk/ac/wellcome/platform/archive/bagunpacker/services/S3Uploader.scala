package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import uk.ac.wellcome.storage.ObjectLocation

class S3Uploader(implicit s3Client: AmazonS3) {

  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  def putObject(
    inputStream: InputStream,
    streamLength: Long,
    uploadLocation: ObjectLocation
  ): UploadResult = {
    val metadata = new ObjectMetadata()
    metadata.setContentLength(streamLength)

    val upload: Upload = transferManager.upload(
      uploadLocation.namespace,
      uploadLocation.key,
      inputStream,
      metadata
    )

    upload.waitForUploadResult()
  }
}
