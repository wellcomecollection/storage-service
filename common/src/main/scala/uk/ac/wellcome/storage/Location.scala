package uk.ac.wellcome.storage

trait Location

case class S3ObjectLocation(
  bucket: String,
  key: String
) extends Location

case object S3ObjectLocation {
  def apply(location: ObjectLocation): S3ObjectLocation =
    S3ObjectLocation(
      bucket = location.namespace,
      key = location.path
    )
}
