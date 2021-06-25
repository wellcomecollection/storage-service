package weco.storage_service.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl.{keywordField, objectField, properties}
import com.sksamuel.elastic4s.analysis.Analysis
import com.sksamuel.elastic4s.fields.{ElasticField, KeywordField}
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import weco.elasticsearch.{ElasticFieldOps, IndexConfig}

/** Helpers for creating Elasticsearch index definitions based on
  * the display models.
  *
  */
trait StorageServiceIndexConfig extends ElasticFieldOps {
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

  lazy val config: IndexConfig = IndexConfig(
    mapping = properties(fields).dynamic(DynamicMapping.Strict),
    analysis = Analysis(analyzers = List())
  )
}
