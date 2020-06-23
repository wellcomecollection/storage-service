package uk.ac.wellcome.storage
import java.nio.file.Paths

trait Location {
  def toObjectLocation: ObjectLocation
}

trait Prefix[OfLocation <: Location] {
  def asLocation(parts: String*): OfLocation

  def toObjectLocationPrefix: ObjectLocationPrefix
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

case class MemoryLocation(
  namespace: String,
  path: String
) extends Location {
  override def toObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = path
    )
}

case object MemoryLocation {
  def apply(location: ObjectLocation): MemoryLocation =
    MemoryLocation(
      namespace = location.namespace,
      path = location.path
    )
}

case class MemoryLocationPrefix(
  namespace: String,
  pathPrefix: String
) extends Prefix[MemoryLocation] {
  override def asLocation(parts: String*): MemoryLocation =
    MemoryLocation(
      namespace = namespace,
      path = Paths.get(pathPrefix, parts: _*).normalize().toString
    )

  override def toObjectLocationPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = namespace,
      path = pathPrefix
    )
}
