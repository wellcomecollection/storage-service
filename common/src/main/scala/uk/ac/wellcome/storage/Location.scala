package uk.ac.wellcome.storage
import java.nio.file.Paths

trait Location

trait Prefix[OfLocation <: Location] {
  def asLocation(parts: String*): OfLocation
}

case class S3ObjectLocation(
  bucket: String,
  key: String
) extends Location {
  def toObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = this.bucket,
      path = this.key
    )
}

case object S3ObjectLocation {
  def apply(location: ObjectLocation): S3ObjectLocation =
    S3ObjectLocation(
      bucket = location.namespace,
      key = location.path
    )
}

case class S3ObjectLocationPrefix(
  bucket: String,
  keyPrefix: String
) extends Prefix[S3ObjectLocation] {
    override def asLocation(parts: String*): S3ObjectLocation =
      S3ObjectLocation(
        bucket = bucket,
        key = Paths.get(keyPrefix, parts: _*).normalize().toString
      )
  }
