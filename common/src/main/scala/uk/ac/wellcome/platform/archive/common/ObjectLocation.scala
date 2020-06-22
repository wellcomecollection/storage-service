package uk.ac.wellcome.storage

import java.nio.file.Paths

import uk.ac.wellcome.storage.s3.S3ObjectLocation

case class ObjectLocation(namespace: String, path: String) extends Location {
  override def toString = s"$namespace/$path"

  def join(parts: String*): ObjectLocation = this.copy(
    path = Paths.get(this.path, parts: _*).normalize().toString
  )

  def asPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = namespace,
      path = path
    )
}

case class ObjectLocationPrefix(namespace: String, path: String) extends Prefix[ObjectLocation] {
  override def toString = s"$namespace/$path"

  def asLocation(parts: String*): ObjectLocation =
    ObjectLocation(namespace, path).join(parts: _*)
}

// TODO: Move this into scala-storage
object Joiner {
  def join(s3ObjectLocation: S3ObjectLocation, path: String): S3ObjectLocation =
    S3ObjectLocation(
      bucket = s3ObjectLocation.bucket,
      key = Paths.get(s3ObjectLocation.key, path).normalize().toString
    )
}
