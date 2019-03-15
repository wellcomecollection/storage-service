package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import uk.ac.wellcome.storage.ObjectLocation

class S3Uploader(bufferSize: Int)(implicit s3Client: AmazonS3) {

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

    val putObjectRequest = new PutObjectRequest(
      uploadLocation.namespace,
      uploadLocation.key,
      inputStream,
      metadata
    )

    // If a transfer fails, the AWS SDK tries to retry a failed transfer by
    // rewinding the input stream to a known-good point, then retrying the
    // bytes it hasn't successfully uploaded yet.
    //
    // The "ReadLimit" parameter tells the SDK how far it can rewind the stream.
    //
    // We use a BufferedInputStream to allow rewinding the stream, so we need
    // to make sure the SDK doesn't try to rewind beyond the end of the buffer.
    // Hence we use this config option to ensure (buffer size) > (rewind limit).
    //
    // See also: bagunpacker.storage.Archive.
    // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/best-practices.html
    //
    val requestClientOptions = putObjectRequest.getRequestClientOptions

    requestClientOptions
      .setReadLimit(bufferSize)

    // Match the readLimit for the multipart upload threshold
    // As per:
    // https://github.com/aws/aws-sdk-java/issues/427#issuecomment-162082586

    transferManager.getConfiguration
      .setMultipartUploadThreshold(
        bufferSize.toLong
      )

    val upload: Upload = transferManager
      .upload(putObjectRequest)

    upload.waitForUploadResult()
  }
}
