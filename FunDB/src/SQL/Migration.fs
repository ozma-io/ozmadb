module FunWithFlags.FunDB.SQL.Migration

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST

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
    | SODeleteConstraint (_, _) -> 1
    | SODeleteTable _ -> 2
    | SODeleteSequence _ -> 3
    | SODeleteSchema _ -> 4
    | SOCreateSchema _ -> 5
    | SOCreateTable _ -> 6
    | SOCreateSequence _ -> 7
    | SOAlterTable _ -> 8
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 9
    | SOCreateConstraint (_, _, CMUnique _) -> 10
    | SOCreateConstraint (_, _, CMForeignKey (_, _)) -> 11
    | SOCreateConstraint (_, _, CMCheck _) -> 12

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

let migrateDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy schemaOperationOrder
