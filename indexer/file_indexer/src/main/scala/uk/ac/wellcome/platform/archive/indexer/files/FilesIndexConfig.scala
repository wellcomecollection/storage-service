package uk.ac.wellcome.platform.archive.indexer.files

import com.sksamuel.elastic4s.ElasticDsl.{
  dateField,
  keywordField,
  longField,
  objectField
}
import com.sksamuel.elastic4s.fields.ElasticField
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.StorageServiceIndexConfig

object FilesIndexConfig extends StorageServiceIndexConfig {
  private val locationFields: Seq[ElasticField] = Seq(
    keywordField("bucket"),
    keywordField("key")
  )

  private val checksumFields: Seq[ElasticField] = Seq(
    keywordField("algorithm"),
    keywordField("value")
  )

  override protected val fields: Seq[ElasticField] =
    Seq(
      keywordField("space"),
      keywordField("externalIdentifier"),
      objectField("location").fields(locationFields: _*),
      keywordField("name"),
      keywordField("suffix"),
      longField("size"),
      objectField("checksum").fields(checksumFields: _*),
      dateField("createdDate")
    )
}
