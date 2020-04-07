package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import com.sksamuel.elastic4s.requests.mappings.{FieldDefinition, KeywordField}

/** Helpers for creating Elasticsearch index definitions based on
  * the display models.
  *
  */
trait IndexConfig {
  protected val displayProviderMappingFields: Seq[KeywordField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  protected val displayLocationFields: Seq[FieldDefinition] =
    Seq(
      objectField("provider").fields(displayProviderMappingFields),
      keywordField("bucket"),
      keywordField("path"),
      keywordField("type")
    )

  protected val displayBagInfoFields: Seq[FieldDefinition] =
    Seq(
      keywordField("externalIdentifier"),
      keywordField("version"),
      keywordField("type")
    )

  protected val fields: Seq[FieldDefinition]

  val mapping = properties(fields).dynamic(DynamicMapping.Strict)
}
