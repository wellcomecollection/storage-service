package uk.ac.wellcome.platform.storage.replica_aggregator

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.DynamoFormat
import uk.ac.wellcome.storage.dynamo.{DynamoConfig, DynamoHashRangeEntry}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashRangeStore


// TODO: Should be in storage library
class DynamoVersionedStore[Id, T](val config: DynamoConfig)(
  implicit
    val client: AmazonDynamoDB,
    val formatHashKey: DynamoFormat[Id],
    val formatRangeKey: DynamoFormat[Int],
    val formatT: DynamoFormat[T],
    val formatDynamoHashRangeEntry: DynamoFormat[DynamoHashRangeEntry[Id, Int, T]]
) extends VersionedStore[Id, Int, T](
  new DynamoHashRangeStore[Id, Int, T](config)
)