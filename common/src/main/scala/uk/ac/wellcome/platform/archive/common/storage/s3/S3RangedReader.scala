package uk.ac.wellcome.platform.archive.common.storage.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3RangedReader(implicit s3Client: AmazonS3) extends Logging {
  def getBytes(
    location: S3ObjectLocation,
    offset: Long,
    count: Long,
    totalLength: Long
  ): Array[Byte] = {
    val getRequest =
      new GetObjectRequest(location.bucket, location.key)

    // The S3 Range request is *inclusive* of the boundaries.
    //
    // For example, if you read (start=0, end=5), you get bytes [0, 1, 2, 3, 4, 5].
    // We never want to read more than bufferSize bytes at a time.
    val requestWithRange =
    if (offset + count >= totalLength) {
      debug(s"Reading $location: $offset-[end] / $totalLength")
      getRequest.withRange(offset)
    } else {
      val endRange = offset + count - 1
      debug(s"Reading $location: $offset-$endRange / $totalLength")
      getRequest.withRange(offset, endRange)
    }

    // Remember to close the input stream afterwards, or we get errors like
    //
    //    com.amazonaws.SdkClientException: Unable to execute HTTP request:
    //    Timeout waiting for connection from pool
    //
    val s3InputStream = s3Client.getObject(requestWithRange).getObjectContent
    val byteArray = IOUtils.toByteArray(s3InputStream)
    s3InputStream.close()

    byteArray
  }
}
