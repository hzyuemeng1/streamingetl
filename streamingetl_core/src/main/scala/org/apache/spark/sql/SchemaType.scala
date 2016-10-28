package org.apache.spark.sql

import org.apache.spark.sql.types._

/**
  * Created by hzyuemeng1 on 2016/10/25.
  */
class SchemaType(schema:StructType) extends StructType(schema.fields) {

  override def add(field: StructField): StructType = schema.add(field)

  override def add(name: String, dataType: DataType): StructType = schema.add(name, dataType)

  override def add(name: String, dataType: DataType, nullable: Boolean): StructType = schema.add(name, dataType, nullable)


  override def add(name: String, dataType: String): StructType = schema.add(name, dataType)

  override def add(name: String, dataType: String, nullable: Boolean): StructType = schema.add(name, dataType, nullable)

  override def add(name: String, dataType: String, nullable: Boolean, metadata: Metadata): StructType = schema.add(name, dataType, nullable, metadata)


  /**
    * Extracts the [[StructField]] with the given name.
    *
    * @throws IllegalArgumentException if a field with the given name does not exist
    */
  override def apply(name: String): StructField = schema.apply(name)


  /**
    * Returns a [[StructType]] containing [[StructField]]s of the given names, preserving the
    * original order of fields.
    *
    * @throws IllegalArgumentException if a field cannot be found for any of the given names
    */
  override def apply(names: Set[String]): StructType = schema.apply(names)


  /**
    * Merges with another schema (`StructType`).  For a struct field A from `this` and a struct field
    * B from `that`,
    *
    * 1. If A and B have the same name and data type, they are merged to a field C with the same name
    *    and data type.  C is nullable if and only if either A or B is nullable.
    * 2. If A doesn't exist in `that`, it's included in the result schema.
    * 3. If B doesn't exist in `this`, it's also included in the result schema.
    * 4. Otherwise, `this` and `that` are considered as conflicting schemas and an exception would be
    *    thrown.
    */
  override private[sql] def merge(that: StructType): StructType = schema.merge(that)


 def getSchema:StructType = schema

}
