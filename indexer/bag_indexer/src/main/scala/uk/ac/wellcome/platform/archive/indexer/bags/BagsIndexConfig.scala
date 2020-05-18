package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl.{dateField, keywordField}
import com.sksamuel.elastic4s.requests.mappings.FieldDefinition
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object BagsIndexConfig extends IndexConfig {
  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordField("id"),
      dateField("createdDate"),
      keywordField("type"),
    )
}