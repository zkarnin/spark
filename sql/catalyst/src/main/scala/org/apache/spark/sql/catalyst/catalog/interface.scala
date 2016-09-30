/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.catalog

import java.util.Date
<<<<<<< HEAD
=======
import javax.annotation.Nullable
>>>>>>> tuning_adaptive

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.StructType


/**
 * A function defined in the catalog.
 *
 * @param identifier name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 * @param resources resource types and Uris used by the function
 */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource])


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class CatalogStorageFormat(
    locationUri: Option[String],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
<<<<<<< HEAD
    properties: Map[String, String]) {

  override def toString: String = {
    val serdePropsToString =
      if (properties.nonEmpty) {
        s"Properties: " + properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
=======
    serdeProperties: Map[String, String]) {

  override def toString: String = {
    val serdePropsToString =
      if (serdeProperties.nonEmpty) {
        s"Properties: " + serdeProperties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
>>>>>>> tuning_adaptive
      } else {
        ""
      }
    val output =
      Seq(locationUri.map("Location: " + _).getOrElse(""),
        inputFormat.map("InputFormat: " + _).getOrElse(""),
        outputFormat.map("OutputFormat: " + _).getOrElse(""),
        if (compressed) "Compressed" else "",
        serde.map("Serde: " + _).getOrElse(""),
        serdePropsToString)
    output.filter(_.nonEmpty).mkString("Storage(", ", ", ")")
  }
<<<<<<< HEAD

}

object CatalogStorageFormat {
  /** Empty storage format for default values and copies. */
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
=======

}

object CatalogStorageFormat {
  /** Empty storage format for default values and copies. */
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, serdeProperties = Map.empty)
}

/**
 * A column in a table.
 */
case class CatalogColumn(
    name: String,
    // This may be null when used to create views. TODO: make this type-safe; this is left
    // as a string due to issues in converting Hive varchars to and from SparkSQL strings.
    @Nullable dataType: String,
    nullable: Boolean = true,
    comment: Option[String] = None) {

  override def toString: String = {
    val output =
      Seq(s"`$name`",
        dataType,
        if (!nullable) "NOT NULL" else "",
        comment.map("(" + _ + ")").getOrElse(""))
    output.filter(_.nonEmpty).mkString(" ")
  }

>>>>>>> tuning_adaptive
}

/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 * @param parameters some parameters for the partition, for example, stats.
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty)


/**
 * A container for bucketing information.
 * Bucketing is a technology for decomposing data sets into more manageable parts, and the number
 * of buckets is fixed so it does not fluctuate with data.
 *
 * @param numBuckets number of buckets.
 * @param bucketColumnNames the names of the columns that used to generate the bucket id.
 * @param sortColumnNames the names of the columns that used to sort data in each bucket.
 */
case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String]) {
  if (numBuckets <= 0) {
    throw new AnalysisException(s"Expected positive number of buckets, but got `$numBuckets`.")
  }
}

/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 *
<<<<<<< HEAD
 * @param provider the name of the data source provider for this table, e.g. parquet, json, etc.
 *                 Can be None if this table is a View, should be "hive" for hive serde tables.
=======
>>>>>>> tuning_adaptive
 * @param unsupportedFeatures is a list of string descriptions of features that are used by the
 *        underlying table but not supported by Spark SQL yet.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
<<<<<<< HEAD
    bucketSpec: Option[BucketSpec] = None,
=======
    sortColumnNames: Seq[String] = Seq.empty,
    bucketColumnNames: Seq[String] = Seq.empty,
    numBuckets: Int = -1,
>>>>>>> tuning_adaptive
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    properties: Map[String, String] = Map.empty,
    stats: Option[Statistics] = None,
    viewOriginalText: Option[String] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty) {

  /** schema of this table's partition columns */
  def partitionSchema: StructType = StructType(schema.filter {
    c => partitionColumnNames.contains(c.name)
  })

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[String] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
<<<<<<< HEAD
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
=======
      locationUri, inputFormat, outputFormat, serde, compressed, serdeProperties))
>>>>>>> tuning_adaptive
  }

  override def toString: String = {
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
<<<<<<< HEAD
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    val bucketStrings = bucketSpec match {
      case Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames)) =>
        val bucketColumnsString = bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        val sortColumnsString = sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        Seq(
          s"Num Buckets: $numBuckets",
          if (bucketColumnNames.nonEmpty) s"Bucket Columns: $bucketColumnsString" else "",
          if (sortColumnNames.nonEmpty) s"Sort Columns: $sortColumnsString" else ""
        )

      case _ => Nil
    }
=======
    val partitionColumns = partitionColumnNames.map("`" + _ + "`").mkString("[", ", ", "]")
    val sortColumns = sortColumnNames.map("`" + _ + "`").mkString("[", ", ", "]")
    val bucketColumns = bucketColumnNames.map("`" + _ + "`").mkString("[", ", ", "]")
>>>>>>> tuning_adaptive

    val output =
      Seq(s"Table: ${identifier.quotedString}",
        if (owner.nonEmpty) s"Owner: $owner" else "",
        s"Created: ${new Date(createTime).toString}",
        s"Last Access: ${new Date(lastAccessTime).toString}",
        s"Type: ${tableType.name}",
        if (schema.nonEmpty) s"Schema: ${schema.mkString("[", ", ", "]")}" else "",
<<<<<<< HEAD
        if (provider.isDefined) s"Provider: ${provider.get}" else "",
        if (partitionColumnNames.nonEmpty) s"Partition Columns: $partitionColumns" else ""
      ) ++ bucketStrings ++ Seq(
=======
        if (partitionColumnNames.nonEmpty) s"Partition Columns: $partitionColumns" else "",
        if (numBuckets != -1) s"Num Buckets: $numBuckets" else "",
        if (bucketColumnNames.nonEmpty) s"Bucket Columns: $bucketColumns" else "",
        if (sortColumnNames.nonEmpty) s"Sort Columns: $sortColumns" else "",
>>>>>>> tuning_adaptive
        viewOriginalText.map("Original View: " + _).getOrElse(""),
        viewText.map("View: " + _).getOrElse(""),
        comment.map("Comment: " + _).getOrElse(""),
        if (properties.nonEmpty) s"Properties: $tableProperties" else "",
<<<<<<< HEAD
        if (stats.isDefined) s"Statistics: ${stats.get}" else "",
=======
>>>>>>> tuning_adaptive
        s"$storage")

    output.filter(_.nonEmpty).mkString("CatalogTable(\n\t", "\n\t", ")")
  }

}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val VIEW = new CatalogTableType("VIEW")
}


/**
 * A database defined in the catalog.
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: String,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]
}


/**
 * An interface that is implemented by logical plans to return the underlying catalog table.
 * If we can in the future consolidate SimpleCatalogRelation and MetastoreRelation, we should
 * probably remove this interface.
 */
trait CatalogRelation {
  def catalogTable: CatalogTable
  def output: Seq[Attribute]
}


/**
 * A [[LogicalPlan]] that wraps [[CatalogTable]].
 *
 * Note that in the future we should consolidate this and HiveCatalogRelation.
 */
case class SimpleCatalogRelation(
    databaseName: String,
    metadata: CatalogTable)
  extends LeafNode with CatalogRelation {

  override def catalogTable: CatalogTable = metadata

  override lazy val resolved: Boolean = false

  override val output: Seq[Attribute] = {
    val (partCols, dataCols) = metadata.schema.toAttributes
      // Since data can be dumped in randomly with no validation, everything is nullable.
      .map(_.withNullability(true).withQualifier(Some(metadata.identifier.table)))
      .partition { a =>
        metadata.partitionColumnNames.contains(a.name)
      }
    dataCols ++ partCols
  }

  require(
    metadata.identifier.database == Some(databaseName),
    "provided database does not match the one specified in the table definition")
}
