package weco.storage.transfer.fixtures

//import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import weco.storage.fixtures.S3Fixtures

import java.net.URI

trait S3TransferManagerFixtures extends S3Fixtures {
  def createS3TransferManagerWithEndpoint(endpoint: String): S3TransferManager = {
    val s3AsyncClient =
      S3CrtAsyncClient.builder()
        .credentialsProvider(s3Credentials)
        .forcePathStyle(true)
        .endpointOverride(new URI(endpoint))
        .build()

    S3TransferManager.builder()
      .s3Client(s3AsyncClient)
      .build()
  }

  implicit val s3TransferManager: S3TransferManager =
    createS3TransferManagerWithEndpoint(s"http://localhost:$s3Port")

  val brokenS3TransferManager: S3TransferManager =
    createS3TransferManagerWithEndpoint("http://nope.nope")
}
