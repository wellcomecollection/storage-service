package uk.ac.wellcome.platform.archive.common.storage

import uk.ac.wellcome.storage.ObjectLocation

trait Resolvable[T] {
  def resolve(root: ObjectLocation)(locatable: T): ObjectLocation
}

object Resolvable {
  implicit def resolvable[T](implicit resolver: T => ObjectLocation) =
    new Resolvable[T] {
      override def resolve(root: ObjectLocation)(locatable: T): ObjectLocation =
        resolver(locatable)
    }

  implicit class Resolver[T](t: T)(
    implicit resolver: Resolvable[T]
  ) {
    def resolve(root: ObjectLocation): ObjectLocation =
      resolver.resolve(root)(t)
  }
}
