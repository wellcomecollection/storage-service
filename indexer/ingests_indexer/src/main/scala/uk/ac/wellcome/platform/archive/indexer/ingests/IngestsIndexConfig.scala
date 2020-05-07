package uk.ac.wellcome.platform.archive.indexer.ingests

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.{FieldDefinition, KeywordField}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object IngestsIndexConfig extends IndexConfig {
  private val displayStatusFields: Seq[KeywordField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  private val displayCallbackFields: Seq[FieldDefinition] =
    Seq(
      keywordField("url"),
      objectField("status").fields(displayStatusFields),
      keywordField("type")
    )

  private val displayIngestTypeFields: Seq[FieldDefinition] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  private val displayIngestEventFields: Seq[FieldDefinition] =
    Seq(
      textField("description"),
      dateField("createdDate"),
      keywordField("type")
    )

  private val bagFields: Seq[FieldDefinition] =
    Seq(
      objectField("info").fields(displayBagInfoFields),
      keywordField("type")
    )

  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordField("id"),
      objectField("sourceLocation").fields(displayLocationFields),
      objectField("callback").fields(displayCallbackFields),
      objectField("ingestType").fields(displayIngestTypeFields),
      objectField("space").fields(displaySpaceFields),
      objectField("status").fields(displayStatusFields),
      objectField("bag").fields(bagFields),
      objectField("events").fields(displayIngestEventFields),
      dateField("createdDate"),
      dateField("lastModifiedDate"),
      keywordField("type"),
      textField("failureDescriptions")
    )
}
