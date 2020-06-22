package uk.ac.wellcome.storage

import java.nio.file.Paths

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
