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

package org.apache.spark.sql.xsql

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{
  InsertIntoTable,
  LogicalPlan,
  SubqueryAlias,
  View
}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.hive.{
  DetermineTableStats,
  HiveMetastoreCatalog,
  HiveStrategies,
  RelationConversions,
  ResolveHiveSerdeTable
}
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}
import org.apache.spark.sql.xsql.execution.XSQLSqlParser
import org.apache.spark.sql.xsql.execution.datasources.{
  XSQLDataSourceAnalysis,
  XSQLFindDataSourceTable
}
import org.apache.spark.sql.xsql.execution.datasources.mysql.TransmitOriginalQuery
import org.apache.spark.sql.xsql.execution.datasources.redis.RedisSpecialStrategy

/**
 * Builder that produces a XSQL-aware `SessionState`.
 */
@Experimental
@InterfaceStability.Unstable
class XSQLSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  private def externalCatalog: ExternalCatalog = session.sharedState.externalCatalog

  override protected def newBuilder: NewBuilder = new XSQLSessionStateBuilder(_, _)

  override protected lazy val sqlParser: ParserInterface = {
    extensions.buildParser(session, new XSQLSqlParser(conf))
  }

  /**
   * Create a [[XSQLSessionCatalog]].
   */
  override protected lazy val catalog: XSQLSessionCatalog = {
    val catalog = new XSQLSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      new HiveMetastoreCatalog(session),
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    // Some configuration configured until method parse of some DataSourceManager called.
    // So we need merge.
    mergeSparkConf(conf, session.sparkContext.conf)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * A logical query plan `Analyzer` with rules specific to XSQL.
   */
  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new ResolveHiveSerdeTable(session) +:
        new RedisSpecialStrategy(session) +:
        new ResolveInputDataStream(session) +:
        new ResolveScanSingleTable(session) +:
        new XSQLFindDataSourceTable(session) +:
        new TransmitOriginalQuery(session) +:
        new ResolveSQLOnFile(session) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      new DetermineTableStats(session) +:
        new ConstructStreamingQuery(session) +:
        RelationConversions(conf, catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        XSQLDataSourceAnalysis(conf) +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        PreReadCheck +:
        customCheckRules
    override lazy val batches: Seq[Batch] = Seq(
      Batch(
        "Hints",
        fixedPoint,
        new ResolveHints.ResolveBroadcastHints(conf),
        ResolveHints.ResolveCoalesceHints,
        ResolveHints.RemoveAllHints),
      Batch("Simple Sanity Check", Once, LookupFunctions),
      Batch(
        "Substitution",
        fixedPoint,
        CTESubstitution,
        WindowsSubstitution,
        EliminateUnions,
        new SubstituteUnresolvedOrdinals(conf)),
      Batch(
        "Resolution",
        fixedPoint,
        ResolveTableValuedFunctions ::
          XSQLResolveRelations ::
          ResolveReferences ::
          ResolveCreateNamedStruct ::
          ResolveDeserializer ::
          ResolveNewInstance ::
          ResolveUpCast ::
          ResolveGroupingAnalytics ::
          ResolvePivot ::
          ResolveOrdinalInOrderByAndGroupBy ::
          ResolveAggAliasInGroupBy ::
          ResolveMissingReferences ::
          ExtractGenerator ::
          ResolveGenerate ::
          ResolveFunctions ::
          ResolveAliases ::
          ResolveSubquery ::
          ResolveSubqueryColumnAliases ::
          ResolveWindowOrder ::
          ResolveWindowFrame ::
          ResolveNaturalAndUsingJoin ::
          ResolveOutputRelation ::
          ExtractWindowExpressions ::
          GlobalAggregates ::
          ResolveAggregateFunctions ::
          TimeWindowing ::
          ResolveInlineTables(conf) ::
          ResolveHigherOrderFunctions(catalog) ::
          ResolveLambdaVariables(conf) ::
          ResolveTimeZone(conf) ::
          ResolveRandomSeed ::
          TypeCoercion.typeCoercionRules(conf) ++
          extendedResolutionRules: _*),
      Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
      Batch("View", Once, AliasViewChild(conf)),
      Batch("Nondeterministic", Once, PullOutNondeterministic),
      Batch("UDF", Once, HandleNullInputsForUDF),
      Batch("FixNullability", Once, FixNullability),
      Batch("Subquery", Once, UpdateOuterReferences),
      Batch("Cleanup", fixedPoint, CleanupAliases))
  }

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) with HiveStrategies {

      override def strategies: Seq[Strategy] =
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++ (PythonEvals ::
          DataSourceV2Strategy ::
          XSQLFileSourceStrategy ::
          DataSourceStrategy(conf) ::
          SpecialLimits ::
          Aggregation ::
          Window ::
          JoinSelection ::
          InMemoryScans ::
          BasicOperators :: Nil)

      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies ++ Seq(HiveTableScans, Scripts)
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object XSQLResolveRelations extends Rule[LogicalPlan] {

    // If the unresolved relation is running directly on files, we just return the original
    // UnresolvedRelation, the plan will get resolved later. Else we look up the table from catalog
    // and change the default database name(in AnalysisContext) if it is a view.
    // We usually look up a table from the default database if the table identifier has an empty
    // database part, for a view the default database should be the currentDb when the view was
    // created. When the case comes to resolving a nested view, the view may have different default
    // database with that the referenced view has, so we need to use
    // `AnalysisContext.defaultDatabase` to track the current default database.
    // When the relation we resolve is a view, we fetch the view.desc(which is a CatalogTable), and
    // then set the value of `CatalogTable.viewDefaultDatabase` to
    // `AnalysisContext.defaultDatabase`, we look up the relations that the view references using
    // the default database.
    // For example:
    // |- view1 (defaultDatabase = db1)
    //   |- operator
    //     |- table2 (defaultDatabase = db1)
    //     |- view2 (defaultDatabase = db2)
    //        |- view3 (defaultDatabase = db3)
    //   |- view4 (defaultDatabase = db4)
    // In this case, the view `view1` is a nested view, it directly references `table2`, `view2`
    // and `view4`, the view `view2` references `view3`. On resolving the table, we look up the
    // relations `table2`, `view2`, `view4` using the default database `db1`, and look up the
    // relation `view3` using the default database `db2`.
    //
    // Note this is compatible with the views defined by older versions of Spark(before 2.2), which
    // have empty defaultDatabase and all the relations in viewText have database part defined.
    def resolveRelation(plan: LogicalPlan): LogicalPlan = plan match {
      case u: UnresolvedRelation if !isRunningDirectlyOnFiles(u.tableIdentifier) =>
        val defaultDataSource = XSQLAnalysisContext.get.defaultDataSource
        val defaultDatabase = XSQLAnalysisContext.get.defaultDatabase
        val foundRelation = lookupTableFromCatalog(u, defaultDataSource, defaultDatabase)
        resolveRelation(foundRelation)
      // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
      // `viewText` should be defined, or else we throw an error on the generation of the View
      // operator.
      // TODO case view @ View(desc, _, child) if !child.resolved =>
      // Resolve all the UnresolvedRelations and Views in the child.
//        val newChild = XSQLAnalysisContext.withAnalysisContext(None, desc.viewDefaultDatabase) {
//          if (XSQLAnalysisContext.get.nestedViewDepth > conf.maxNestedViewDepth) {
//          view.failAnalysis(s"The depth of view ${view.desc.identifier} exceeds the maximum " +
//          s"view resolution depth (${conf.maxNestedViewDepth}). Analysis is aborted to " +
//          s"avoid errors. Increase the value of ${SQLConf.MAX_NESTED_VIEW_DEPTH.key} to work " +
//          "around this.")
//          }
//          executeSameContext(child)
//        }
//        view.copy(child = newChild)
      case p @ SubqueryAlias(_, view: View) =>
        val newChild = resolveRelation(view)
        p.copy(child = newChild)
      case _ => plan
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case i @ InsertIntoTable(u: UnresolvedRelation, parts, child, _, _) if child.resolved =>
        EliminateSubqueryAliases(lookupTableFromCatalog(u)) match {
          case v: View =>
            u.failAnalysis(s"Inserting into a view is not allowed. View: ${v.desc.identifier}.")
          case other => i.copy(table = other)
        }
      case u: UnresolvedRelation => resolveRelation(u)
    }

    // Look up the table with the given name from catalog. The database we used is decided by the
    // precedence:
    // 1. Use the database part of the table identifier, if it is defined;
    // 2. Use defaultDatabase, if it is defined(In this case, no temporary objects can be used,
    //    and the default database is only used to look up a view);
    // 3. Use the currentDb of the SessionCatalog.
    private def lookupTableFromCatalog(
        u: UnresolvedRelation,
        defaultDataSource: Option[String] = None,
        defaultDatabase: Option[String] = None): LogicalPlan = {
      val tableIdentWithDb = u.tableIdentifier.copy(
        dataSource = u.tableIdentifier.dataSource.orElse(defaultDataSource),
        database = u.tableIdentifier.database.orElse(defaultDatabase))
      try {
        catalog.lookupRelation(tableIdentWithDb)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table or view not found: ${tableIdentWithDb.unquotedString}")
        // If the database is defined and that database is not found, throw an AnalysisException.
        // Note that if the database is not defined, it is possible we are looking up a temp view.
        case e: NoSuchDatabaseException =>
          u.failAnalysis(s"Database not found: ${e.db}")
      }
    }

    // If the database part is specified, and we support running SQL directly on files, and
    // it's not a temporary view, and the table does not exist, then let's just return the
    // original UnresolvedRelation. It is possible we are matching a query like "select *
    // from parquet.`/path/to/query`". The plan will get resolved in the rule `ResolveDataSource`.
    // Note that we are testing (!db_exists || !table_exists) because the catalog throws
    // an exception from tableExists if the database does not exist.
    private def isRunningDirectlyOnFiles(table: TableIdentifier): Boolean = {
      table.database.isDefined && conf.runSQLonFile && !catalog.isTemporaryTable(table) &&
      (!catalog.databaseExists(table.dataSource, table.database.get) ||
      !catalog.tableExists(table))
    }
  }

}

/**
 * Provides a way to keep state during the analysis, this enables us to decouple the concerns
 * of analysis environment from the catalog.
 * The state that is kept here is per-query.
 *
 * Note this is thread local.
 *
 * @param defaultDatabase The default database used in the view resolution, this overrules the
 *                        current catalog database.
 * @param nestedViewDepth The nested depth in the view resolution, this enables us to limit the
 *                        depth of nested views.
 */
case class XSQLAnalysisContext(
    defaultDataSource: Option[String] = None,
    defaultDatabase: Option[String] = None,
    nestedViewDepth: Int = 0)

object XSQLAnalysisContext {
  private val value = new ThreadLocal[XSQLAnalysisContext]() {
    override def initialValue: XSQLAnalysisContext = XSQLAnalysisContext()
  }

  def get: XSQLAnalysisContext = value.get()
  def reset(): Unit = value.remove()

  private def set(context: XSQLAnalysisContext): Unit = value.set(context)

  def withAnalysisContext[A](dataSource: Option[String], database: Option[String])(f: => A): A = {
    val originContext = value.get()
    val context = XSQLAnalysisContext(
      defaultDataSource = dataSource,
      defaultDatabase = database,
      nestedViewDepth = originContext.nestedViewDepth + 1)
    set(context)
    try f
    finally { set(originContext) }
  }
}
