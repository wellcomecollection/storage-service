package weco.storage_service.bag_tracker.config.builders

import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.dynamo.DynamoStorageManifestDao
import weco.storage.typesafe.{DynamoBuilder, S3Builder}

object StorageManifestDaoBuilder {
  def build(config: Config): StorageManifestDao = {
    implicit val dynamoClient: DynamoDbClient =
      DynamoBuilder.buildDynamoClient

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client

    new DynamoStorageManifestDao(
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config, namespace = "vhs"),
      s3Config = S3Builder.buildS3Config(config, namespace = "vhs")
    )
  }
}
