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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
<<<<<<< HEAD
import org.apache.spark.sql.execution.command.DDLUtils
=======
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils._
import org.apache.spark.sql.execution.command.CreateHiveTableAsSelectLogicalPlan
>>>>>>> tuning_adaptive
import org.apache.spark.sql.execution.datasources.{Partition => _, _}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types._


/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
  private val client =
    sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String)

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  private def getQualifiedTableName(t: CatalogTable): QualifiedTableName = {
    QualifiedTableName(
      t.identifier.database.getOrElse(getCurrentDatabase).toLowerCase,
      t.identifier.table.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
<<<<<<< HEAD
        val table = sparkSession.sharedState.externalCatalog.getTable(in.database, in.name)

        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = Some(table.schema),
            partitionColumns = table.partitionColumnNames,
            bucketSpec = table.bucketSpec,
            className = table.provider.get,
            options = table.storage.properties)

        LogicalRelation(
          dataSource.resolveRelation(),
          catalogTable = Some(table))
=======
        val table = client.getTable(in.database, in.name)

        // TODO: the following code is duplicated with FindDataSourceTable.readDataSourceTable

        def schemaStringFromParts: Option[String] = {
          table.properties.get(DATASOURCE_SCHEMA_NUMPARTS).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"$DATASOURCE_SCHEMA_PART_PREFIX$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        def getColumnNames(colType: String): Seq[String] = {
          table.properties.get(s"$DATASOURCE_SCHEMA.num${colType.capitalize}Cols").map {
            numCols => (0 until numCols.toInt).map { index =>
              table.properties.getOrElse(s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
                throw new AnalysisException(
                  s"Could not read $colType columns from the metastore because it is corrupted " +
                    s"(missing part $index of it, $numCols parts are expected)."))
            }
          }.getOrElse(Nil)
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString = table.properties.get(DATASOURCE_SCHEMA).orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get data types of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = getColumnNames("part")

        val bucketSpec = table.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { n =>
          BucketSpec(n.toInt, getColumnNames("bucket"), getColumnNames("sort"))
        }

        val options = table.storage.serdeProperties
        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = userSpecifiedSchema,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = table.properties(DATASOURCE_PROVIDER),
            options = options)

        LogicalRelation(
          dataSource.resolveRelation(checkPathExist = true),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
>>>>>>> tuning_adaptive
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = sparkSession.sharedState.externalCatalog.getTable(
      qualifiedTableName.database, qualifiedTableName.name)

<<<<<<< HEAD
    if (DDLUtils.isDatasourceTable(table)) {
=======
    if (table.properties.get(DATASOURCE_PROVIDER).isDefined) {
>>>>>>> tuning_adaptive
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable, None)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable, None)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
<<<<<<< HEAD
      SubqueryAlias(
        alias.getOrElse(table.identifier.table),
        sparkSession.sessionState.sqlParser.parsePlan(viewText),
        Option(table.identifier))
=======
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None =>
          SubqueryAlias(table.identifier.table,
            sparkSession.sessionState.sqlParser.parsePlan(viewText))
        case Some(aliasText) =>
          SubqueryAlias(aliasText, sessionState.sqlParser.parsePlan(viewText))
      }
>>>>>>> tuning_adaptive
    } else {
      val qualifiedTable =
        MetastoreRelation(
          qualifiedTableName.database, qualifiedTableName.name)(table, client, sparkSession)
      alias.map(a => SubqueryAlias(a, qualifiedTable, None)).getOrElse(qualifiedTable)
    }
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      pathsInMetastore: Seq[String],
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    cachedDataSourceTables.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                relation.bucketSpec == expectedBucketSpec &&
                relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                  PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
                }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              cachedDataSourceTables.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
                s"should be stored as $expectedFileFormat. However, we are getting " +
                s"a ${relation.fileFormat} from the metastore cache. This cached " +
                s"entry will be invalidated.")
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
            s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
            s"This cached entry will be invalidated.")
        cachedDataSourceTables.invalidate(tableIdentifier)
        None
    }
  }

  private def convertToLogicalRelation(
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val partitionPaths = partitions.map(_.path.toString)

      // By convention (for example, see MetaStorePartitionedTableFileCatalog), the definition of a
      // partitioned table's paths depends on whether that table has any actual partitions.
      // Partitioned tables without partitions use the location of the table's base path.
      // Partitioned tables with partitions use the locations of those partitions' data locations,
      // _omitting_ the table's base path.
      val paths = if (partitionPaths.isEmpty) {
        Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)
      } else {
        partitionPaths
      }

      val cached = getCached(
        tableIdentifier,
        paths,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        Some(partitionSpec))

      val hadoopFsRelation = cached.getOrElse {
        val fileCatalog = new MetaStorePartitionedTableFileCatalog(
          sparkSession,
          new Path(metastoreRelation.catalogTable.storage.locationUri.get),
          partitionSpec)

        val inferredSchema = if (fileType.equals("parquet")) {
          val inferredSchema =
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
          inferredSchema.map { inferred =>
            ParquetFileFormat.mergeMetastoreParquetSchema(metastoreSchema, inferred)
          }.getOrElse(metastoreSchema)
        } else {
          defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
        }

        val relation = HadoopFsRelation(
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = inferredSchema,
          bucketSpec = bucketSpec,
          fileFormat = defaultSource,
          options = options)(sparkSession = sparkSession)

<<<<<<< HEAD
        val created = LogicalRelation(relation, catalogTable = Some(metastoreRelation.catalogTable))
=======
        val created = LogicalRelation(
          relation,
          metastoreTableIdentifier =
            Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))
>>>>>>> tuning_adaptive
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      hadoopFsRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier,
        paths,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        None)
      val logicalRelation = cached.getOrElse {
        val created =
          LogicalRelation(
            DataSource(
              sparkSession = sparkSession,
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              bucketSpec = bucketSpec,
              options = options,
              className = fileType).resolveRelation(),
<<<<<<< HEAD
              catalogTable = Some(metastoreRelation.catalogTable))
=======
              metastoreTableIdentifier =
                Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))
>>>>>>> tuning_adaptive


        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      logicalRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
        sessionState.convertMetastoreParquet
    }

    private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new ParquetFileFormat()
      val fileFormatClass = classOf[ParquetFileFormat]

      val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
      val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
          val parquetRelation = convertToParquetRelation(relation)
          SubqueryAlias(relation.tableName, parquetRelation, None)
      }
    }
  }

  /**
   * When scanning Metastore ORC tables, convert them to ORC data source relations
   * for better performance.
   */
  object OrcConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
        sessionState.convertMetastoreOrc
    }

    private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new OrcFileFormat()
      val fileFormatClass = classOf[OrcFileFormat]
      val options = Map[String, String]()

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
          val orcRelation = convertToOrcRelation(relation)
          SubqueryAlias(relation.tableName, orcRelation, None)
      }
    }
  }
<<<<<<< HEAD
=======

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p

      case p @ CreateHiveTableAsSelectLogicalPlan(table, child, allowExisting) =>
        val desc = if (table.storage.serde.isEmpty) {
          // add default serde
          table.withNewStorage(
            serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        } else {
          table
        }

        val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

        execution.CreateHiveTableAsSelectCommand(
          desc.copy(identifier = TableIdentifier(tblName, Some(dbName))),
          child,
          allowExisting)
    }
  }
>>>>>>> tuning_adaptive
}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 *
 * @param tableBasePath The default base path of the Hive metastore table
 * @param partitionSpec The partition specifications from Hive metastore
 */
private[hive] class MetaStorePartitionedTableFileCatalog(
    sparkSession: SparkSession,
    tableBasePath: Path,
    override val partitionSpec: PartitionSpec)
  extends ListingFileCatalog(
    sparkSession,
    MetaStorePartitionedTableFileCatalog.getPaths(tableBasePath, partitionSpec),
    Map.empty,
    Some(partitionSpec.partitionColumns)) {
<<<<<<< HEAD
}

private[hive] object MetaStorePartitionedTableFileCatalog {
  /** Get the list of paths to list files in the for a metastore table */
  def getPaths(tableBasePath: Path, partitionSpec: PartitionSpec): Seq[Path] = {
    // If there are no partitions currently specified then use base path,
    // otherwise use the paths corresponding to the partitions.
    if (partitionSpec.partitions.isEmpty) {
      Seq(tableBasePath)
    } else {
      partitionSpec.partitions.map(_.path)
    }
=======
}

private[hive] object MetaStorePartitionedTableFileCatalog {
  /** Get the list of paths to list files in the for a metastore table */
  def getPaths(tableBasePath: Path, partitionSpec: PartitionSpec): Seq[Path] = {
    // If there are no partitions currently specified then use base path,
    // otherwise use the paths corresponding to the partitions.
    if (partitionSpec.partitions.isEmpty) {
      Seq(tableBasePath)
    } else {
      partitionSpec.partitions.map(_.path)
    }
  }
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  val numDynamicPartitions = partition.values.count(_.isEmpty)

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions)

  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
>>>>>>> tuning_adaptive
  }
}
