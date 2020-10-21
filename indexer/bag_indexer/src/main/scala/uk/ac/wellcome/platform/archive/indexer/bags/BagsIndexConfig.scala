package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.FieldDefinition

import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object BagsIndexConfig extends IndexConfig {

  private val infoFields: Seq[FieldDefinition] = Seq(
    keywordField("externalIdentifier"),
    keywordField("payloadOxum"),
    dateField("baggingDate"),
    keywordField("sourceOrganisation"),
    keywordField("externalDescription"),
    keywordField("internalSenderIdentifier"),
    keywordField("internalSenderDescription")
  )

  private val locationFields: Seq[FieldDefinition] = Seq(
    keywordField("provider"),
    keywordField("bucket"),
    keywordField("path")
  )

  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordField("id"),
      keywordField("space"),
      intField("version"),
      dateField("createdDate"),
      objectField("info").fields(infoFields),
      objectField("location").fields(locationFields),
      objectField("replicaLocations").fields(locationFields),
      intField("filesCount"),
      longField("filesTotalSize")
    )
}
