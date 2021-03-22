package uk.ac.wellcome.platform.archive.indexer.files

import com.sksamuel.elastic4s.ElasticDsl.{
  dateField,
  keywordField,
  longField,
  objectField
}
import com.sksamuel.elastic4s.requests.mappings.FieldDefinition
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.StorageServiceIndexConfig

object FilesIndexConfig extends StorageServiceIndexConfig {
  private val locationFields: Seq[FieldDefinition] = Seq(
    keywordField("bucket"),
    keywordField("key")
  )

  private val checksumFields: Seq[FieldDefinition] = Seq(
    keywordField("algorithm"),
    keywordField("value")
  )

  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordField("space"),
      keywordField("externalIdentifier"),
      objectField("location").fields(locationFields),
      keywordField("name"),
      keywordField("suffix"),
      longField("size"),
      objectField("checksum").fields(checksumFields),
      dateField("createdDate")
    )
}
