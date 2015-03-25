package org.example.graphx

abstract class Node {
  def properties: Map[PropertyKey, Any]

  def isEmpty: Boolean

  def nodeType: NodeType = ???

  def hasType(candidate: NodeType) = nodeType == candidate
}

case object EmptyNode extends Node {
  override def properties = Map.empty

  override val isEmpty = true
}

case class UntypedNode(properties: Map[PropertyKey, Any] = Map.empty) extends Node {
  override val isEmpty = false
}

case class TypedNode(override val nodeType: NodeType, properties: Map[PropertyKey, Any] = Map.empty) extends Node {
  override val isEmpty = false
}
