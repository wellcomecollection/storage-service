package weco.storage_service.indexer.ingests

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.fields.ElasticField
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

object IngestsIndexConfig extends StorageServiceIndexConfig {
  private val displayStatusFields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  private val displayCallbackFields: Seq[ElasticField] =
    Seq(
      keywordField("url"),
      objectField("status").fields(displayStatusFields: _*),
      keywordField("type")
    )

  private val displayIngestTypeFields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  private val displayIngestEventFields: Seq[ElasticField] =
    Seq(
      textField("description"),
      dateField("createdDate"),
      keywordField("type")
    )

  private val bagFields: Seq[ElasticField] =
    Seq(
      objectField("info").fields(displayBagInfoFields: _*),
      keywordField("type")
    )

  override protected val fields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      objectField("sourceLocation").fields(displayLocationFields: _*),
      objectField("callback").fields(displayCallbackFields: _*),
      objectField("ingestType").fields(displayIngestTypeFields: _*),
      objectField("space").fields(displaySpaceFields: _*),
      objectField("status").fields(displayStatusFields: _*),
      objectField("bag").fields(bagFields: _*),
      objectField("events").fields(displayIngestEventFields: _*),
      dateField("createdDate"),
      dateField("lastModifiedDate"),
      keywordField("type"),
      textField("failureDescriptions")
    )
}
