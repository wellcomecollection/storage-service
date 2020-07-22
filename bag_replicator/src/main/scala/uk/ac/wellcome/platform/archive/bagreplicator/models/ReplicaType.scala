package uk.ac.wellcome.platform.archive.bagreplicator.models

sealed trait ReplicaType

case object PrimaryReplica extends ReplicaType
case object SecondaryReplica extends ReplicaType
