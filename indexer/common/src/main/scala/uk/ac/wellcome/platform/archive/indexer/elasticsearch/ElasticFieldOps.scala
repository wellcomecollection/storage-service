package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.fields.{ElasticField, ObjectField}

trait ElasticFieldOps {

  /** Sometime around 7.11.x, Elastic4s removed FieldDefinition in favour of ElasticField,
    * which also lost a whole bunch of useful helpers from the DSL.
    *
    * These classes are meant to replicate some of the missing functionality, with the
    * hope that they'll be re-added in some future version of Elastic4s, and we can remove
    * these helpers as a no-op.
    */
  implicit class ObjectFieldsOps(of: ObjectField) {
    def fields(fields: Seq[ElasticField]): ObjectField =
      of.copy(properties = fields)
  }
}
