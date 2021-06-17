package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.analysis.Analysis
import com.sksamuel.elastic4s.fields.{ElasticField, KeywordField}
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import uk.ac.wellcome.elasticsearch.IndexConfig
import weco.elasticsearch.ElasticFieldOps

/** Helpers for creating Elasticsearch index definitions based on
  * the display models.
  *
  */
trait StorageServiceIndexConfig extends IndexConfig with ElasticFieldOps {
  protected val displayProviderMappingFields: Seq[KeywordField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  protected val displayLocationFields: Seq[ElasticField] =
    Seq(
      objectField("provider").fields(displayProviderMappingFields: _*),
      keywordField("bucket"),
      keywordField("path"),
      keywordField("type")
    )

  protected val displayBagInfoFields: Seq[ElasticField] =
    Seq(
      keywordField("externalIdentifier"),
      keywordField("version"),
      keywordField("type")
    )

  protected val displaySpaceFields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  protected val fields: Seq[ElasticField]

  def mapping: MappingDefinition =
    properties(fields).dynamic(DynamicMapping.Strict)

  val analysis: Analysis = Analysis(analyzers = List())
}
