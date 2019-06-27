package uk.ac.wellcome.platform.archive.common.config.builders

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.store.dynamo.{DynamoHashStore, DynamoVersionedHybridStore}
import uk.ac.wellcome.storage.typesafe.DynamoBuilder

object StorageManifestDaoBuilder {
  def build(config: Config): StorageManifestDao = {
    implicit val client: AmazonDynamoDB = DynamoBuilder.buildDynamoClient(config)

    val vhs = new DynamoVersionedHybridStore[StorageManifest, Map[String, String]](
      store = new DynamoHashStore(
        config = DynamoBuilder.buildDynamoConfig(config, namespace = "vhs")
      )
    )

    new StorageManifestDao(vhs)
  }
}
