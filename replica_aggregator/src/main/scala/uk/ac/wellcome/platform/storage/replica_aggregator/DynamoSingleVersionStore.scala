package uk.ac.wellcome.platform.storage.replica_aggregator

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.DynamoFormat
import uk.ac.wellcome.storage.dynamo.{DynamoConfig, DynamoHashEntry}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.dynamo.DynamoHashStore

// TODO: Should be in storage library
class DynamoSingleVersionStore[Id, T](val config: DynamoConfig)(
  implicit
  val client: AmazonDynamoDB,
  val formatHashKey: DynamoFormat[Id],
  val formatRangeKey: DynamoFormat[Int],
  val formatT: DynamoFormat[T],
  val formatDynamoHashRangeEntry: DynamoFormat[DynamoHashEntry[Id, Int, T]]
) extends VersionedStore[Id, Int, T](
      new DynamoHashStore[Id, Int, T](config)
    )
