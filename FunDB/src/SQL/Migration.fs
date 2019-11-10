module FunWithFlags.FunDB.SQL.Migration

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Query

type MigrationPlan = SchemaOperation seq

// Order for table operations so that dependencies are not violated.
let private tableOperationOrder = function
    | TODeleteColumn _ -> 1
    | TOCreateColumn _ -> 2
    | TOAlterColumnType _ -> 3
    | TOAlterColumnNull _ -> 4
    | TOAlterColumnDefault _ -> 5

// Order for database operations so that dependencies are not violated.
let private schemaOperationOrder = function
    | SODeleteConstraint _ -> 1
    | SODeleteIndex _ -> 2
    | SODeleteTable _ -> 3
    | SODeleteSequence _ -> 4
    | SODeleteSchema _ -> 5
    | SOCreateSchema _ -> 6
    | SOCreateTable _ -> 7
    | SOCreateSequence _ -> 8
    | SOAlterTable _ -> 9
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 10
    | SOCreateConstraint (_, _, CMUnique _) -> 11
    | SOCreateConstraint (_, _, CMForeignKey _) -> 12
    | SOCreateConstraint (_, _, CMCheck _) -> 13
    | SOCreateIndex _ -> 14

let private deleteBuildTable (table : TableRef) (tableMeta : TableMeta) : MigrationPlan =
    seq { yield SODeleteTable table
        }

let private deleteBuildSchema (schemaName : SchemaName) (schemaMeta : SchemaMeta) : MigrationPlan =
    seq { for KeyValue (objectName, obj) in schemaMeta.objects do
              let objRef = { schema = Some schemaName; name = objectName }
              match obj with
                  | OMTable tableMeta ->
                      yield! deleteBuildTable objRef tableMeta
                  | OMSequence ->
                      yield SODeleteSequence objRef
                  | OMConstraint (tableName, constraintMeta) ->
                      yield SODeleteConstraint (objRef, tableName)
                  | OMIndex (tableName, indexMeta) ->
                      yield SODeleteIndex objRef
          yield SODeleteSchema schemaName
        }

[<NoComparison>]
type AColumnMeta =
    { columnType : DBValueType
      isNullable : bool
      defaultExpr : ValueExpr option
    }

let private migrateBuildTable (fromMeta : TableMeta) (toMeta : TableMeta) : TableOperation seq =
    seq { for KeyValue (columnName, columnMeta) in toMeta.columns do
              match Map.tryFind columnName fromMeta.columns with
                  | None -> yield TOCreateColumn (columnName, columnMeta)
                  | Some oldColumnMeta ->
                      if oldColumnMeta.columnType <> columnMeta.columnType then
                          yield TOAlterColumnType (columnName, columnMeta.columnType)
                      if oldColumnMeta.isNullable <> columnMeta.isNullable then
                          yield TOAlterColumnNull (columnName, columnMeta.isNullable)
                      if oldColumnMeta.defaultExpr <> columnMeta.defaultExpr then
                          yield TOAlterColumnDefault (columnName, columnMeta.defaultExpr)

          for KeyValue (columnName, columnMeta) in fromMeta.columns do
              if not (Map.containsKey columnName toMeta.columns) then
                  yield TODeleteColumn columnName
        }

let migrateAlterTable (objRef : SchemaObject) (fromMeta : TableMeta) (toMeta : TableMeta) : MigrationPlan =
    seq {
        let changes = migrateBuildTable fromMeta toMeta |> Array.ofSeq
        if not <| Array.isEmpty changes then
            yield SOAlterTable (objRef, changes |> Array.sortBy tableOperationOrder)
    }

let private migrateBuildSchema (schema : SchemaName) (fromMeta : SchemaMeta) (toMeta : SchemaMeta) : MigrationPlan =
    seq { for KeyValue (objectName, obj) in toMeta.objects do
              let objRef = { schema = Some schema; name = objectName }
              match obj with
                  | OMTable tableMeta ->
                      match Map.tryFind objectName fromMeta.objects with
                          | Some (OMTable oldTableMeta) ->
                              yield! migrateAlterTable objRef oldTableMeta tableMeta
                          | _ ->
                              yield SOCreateTable objRef
                              yield! migrateAlterTable objRef emptyTableMeta tableMeta
                  | OMSequence ->
                      match Map.tryFind objectName fromMeta.objects with
                          | Some OMSequence -> ()
                          | _ -> yield SOCreateSequence objRef
                  | OMConstraint (tableName, constraintType) ->
                      match Map.tryFind objectName fromMeta.objects with
                          | Some (OMConstraint (oldTableName, oldConstraintType)) ->
                              if tableName <> oldTableName || constraintType <> oldConstraintType then
                                  yield SODeleteConstraint (objRef, oldTableName)
                                  yield SOCreateConstraint (objRef, tableName, constraintType)
                          | _ -> yield SOCreateConstraint (objRef, tableName, constraintType)
                  | OMIndex (tableName, index) ->
                      match Map.tryFind objectName fromMeta.objects with
                          | Some (OMIndex (oldTableName, oldIndex)) ->
                              if tableName <> oldTableName || index <> oldIndex then
                                  yield SODeleteIndex objRef
                                  yield SOCreateIndex (objRef, tableName, index)
                          | _ -> yield SOCreateIndex (objRef, tableName, index)

          for KeyValue (objectName, obj) in fromMeta.objects do
              let objRef = { schema = Some schema; name = objectName }
              match obj with
                  | OMTable tableMeta ->
                      match Map.tryFind objectName toMeta.objects with
                          | Some (OMTable _) -> ()
                          | _ -> yield! deleteBuildTable objRef tableMeta
                  | OMSequence ->
                      match Map.tryFind objectName toMeta.objects with
                          | Some OMSequence -> ()
                          | _ -> yield SODeleteSequence objRef
                  | OMConstraint (tableName, _) ->
                      match Map.tryFind objectName toMeta.objects with
                          | Some (OMConstraint _) -> ()
                          | _ -> yield SODeleteConstraint (objRef, tableName)
                  | OMIndex (tableName, _) ->
                      match Map.tryFind objectName toMeta.objects with
                          | Some (OMIndex _) -> ()
                          | _ -> yield SODeleteIndex objRef
        }

let private migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    seq { for KeyValue (schemaName, schemaMeta) in toMeta.schemas do
              match Map.tryFind schemaName fromMeta.schemas with
                  | None ->
                      yield SOCreateSchema schemaName
                      yield! migrateBuildSchema schemaName emptySchemaMeta schemaMeta
                  | Some oldSchemaMeta -> yield! migrateBuildSchema schemaName oldSchemaMeta schemaMeta

          for KeyValue (schemaName, schemaMeta) in fromMeta.schemas do
              if not (Map.containsKey schemaName toMeta.schemas) then
                  yield! deleteBuildSchema schemaName schemaMeta
        }

let planDatabaseMigration (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy schemaOperationOrder

let migrateDatabase (query : QueryConnection) (plan : MigrationPlan) : Task<bool> =
    task {
        let mutable touched = false
        for action in plan do
            let! _ = query.ExecuteNonQuery (action.ToSQLString()) Map.empty
            touched <- true
            ()
        if touched then
            // Clear prepared statements so that things don't break if e.g. database types have changed.
            query.Connection.UnprepareAll ()
        return touched
    }