package uk.ac.wellcome.platform.archive.indexer.ingests

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.fields.ElasticField
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.StorageServiceIndexConfig

object IngestsIndexConfig extends StorageServiceIndexConfig {
  private val displayStatusFields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      keywordField("type")
    )

  private val displayCallbackFields: Seq[ElasticField] =
    Seq(
      keywordField("url"),
      objectField("status").fields(displayStatusFields),
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
      objectField("info").fields(displayBagInfoFields),
      keywordField("type")
    )

  override protected val fields: Seq[ElasticField] =
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
