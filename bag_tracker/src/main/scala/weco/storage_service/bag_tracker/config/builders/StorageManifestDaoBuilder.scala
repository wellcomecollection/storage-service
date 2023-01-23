package weco.storage_service.bag_tracker.config.builders

import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.s3.S3Client
import weco.storage.typesafe.{DynamoBuilder, S3Builder}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.dynamo.DynamoStorageManifestDao

object StorageManifestDaoBuilder {
  def build(config: Config): StorageManifestDao = {
    implicit val dynamoClient: DynamoDbClient =
      DynamoDbClient.builder().build()

    implicit val s3Client: S3Client =
      S3Client.builder().build()

    new DynamoStorageManifestDao(
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config, namespace = "vhs"),
      s3Config = S3Builder.buildS3Config(config, namespace = "vhs")
    )
  }
}
