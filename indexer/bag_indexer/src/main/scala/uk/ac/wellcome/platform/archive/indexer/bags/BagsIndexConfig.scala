package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.FieldDefinition

import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object BagsIndexConfig extends IndexConfig {

  private val fileFields: Seq[FieldDefinition] =
    Seq(
      keywordField("path"),
      keywordField("name"),
      keywordField("suffix"),
      longField("size"),
      keywordField("checksum"),
      keywordField("type")
    )

  private val infoFields: Seq[FieldDefinition] = Seq(
    keywordField("externalIdentifier"),
    keywordField("payloadOxum"),
    dateField("baggingDate"),
    keywordField("sourceOrganisation"),
    keywordField("externalDescription"),
    keywordField("internalSenderIdentifier"),
    keywordField("internalSenderDescription"),
    keywordField("type")
  )

  private val locationFields: Seq[FieldDefinition] = Seq(
    keywordField("provider"),
    keywordField("bucket"),
    keywordField("path"),
    keywordField("type")
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
      nestedField("files").fields(fileFields),
      intField("filesCount"),
      longField("filesTotalSize"),
      keywordField("type")
    )

}
