package uk.ac.wellcome.platform.archive.bag_tracker.config.builders

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.services.dynamo.DynamoStorageManifestDao
import uk.ac.wellcome.storage.typesafe.{DynamoBuilder, S3Builder}

object StorageManifestDaoBuilder {
  def build(config: Config): StorageManifestDao = {
    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    new DynamoStorageManifestDao(
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config, namespace = "vhs"),
      s3Config = S3Builder.buildS3Config(config, namespace = "vhs")
    )
  }
}
