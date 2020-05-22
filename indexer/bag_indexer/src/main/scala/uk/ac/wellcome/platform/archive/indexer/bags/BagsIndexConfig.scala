package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.FieldDefinition
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object BagsIndexConfig extends IndexConfig {
  private val suffixTallyFields: Seq[FieldDefinition] =
    Seq(
      keywordField("suffix"),
      intField("count")
    )

  private val fileFields: Seq[FieldDefinition] =
    Seq(
      keywordField("path"),
      keywordField("name"),
      keywordField("name"),
      longField("size"),
      keywordField("checksum"),
      keywordField("type")
    )

  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordField("id"),
      keywordField("space"),
      keywordField("externalIdentifier"),
      intField("version"),
      dateField("createdDate"),
      objectField("payloadFiles").fields(fileFields),
      objectField("payloadFileSuffixTally").fields(suffixTallyFields),
      intField("payloadFileCount"),
      longField("payloadFileSize"),
      objectField("newPayloadFileSuffixTally").fields(suffixTallyFields),
      intField("newPayloadFileCount"),
      longField("newPayloadFileSize"),
      keywordField("type")
    )
}
