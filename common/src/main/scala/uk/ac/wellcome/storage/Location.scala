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

  def asPrefix: S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = this.bucket,
      keyPrefix = this.key
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

  def toObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = this.bucket,
      path = this.keyPrefix
    )
}

case object S3ObjectLocationPrefix {
  def apply(location: ObjectLocationPrefix): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = location.namespace,
      keyPrefix = location.path
    )
}
