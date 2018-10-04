module FunWithFlags.FunDB.SQL.Migration

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST

type MigrationPlan = SchemaOperation seq

let private deleteBuildTable (table : TableRef) (tableMeta : TableMeta) : MigrationPlan =
    seq { for KeyValue (columnName, columnMeta) in tableMeta.columns do
              yield SODeleteColumn { table = table; name = columnName }
          yield SODeleteTable table
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

let private migrateBuildTable (table : TableRef) (fromMeta : TableMeta) (toMeta : TableMeta) : MigrationPlan =
    seq { for KeyValue (columnName, columnMeta) in toMeta.columns do
              let objRef = { table = table; name = columnName } : ResolvedColumnRef
              match Map.tryFind columnName fromMeta.columns with
                  | None -> yield SOCreateColumn (objRef, columnMeta)
                  | Some oldColumnMeta ->
                      if oldColumnMeta <> columnMeta then
                          // XXX: Support altering columns instead of recreating them.
                          yield SODeleteColumn objRef
                          yield SOCreateColumn (objRef, columnMeta)

          for KeyValue (columnName, columnMeta) in fromMeta.columns do
              if not (Map.containsKey columnName toMeta.columns) then
                  let objRef = { table = table; name = columnName } : ResolvedColumnRef
                  yield SODeleteColumn objRef
        }

let private migrateBuildSchema (schema : SchemaName option) (fromMeta : SchemaMeta) (toMeta : SchemaMeta) : MigrationPlan =
    seq { for KeyValue (objectName, obj) in toMeta.objects do
              let objRef = { schema = schema; name = objectName }
              match obj with
                  | OMTable tableMeta ->
                      match Map.tryFind objectName fromMeta.objects with
                          | Some (OMTable oldTableMeta) -> yield! migrateBuildTable objRef oldTableMeta tableMeta
                          | _ ->
                              yield SOCreateTable objRef
                              yield! migrateBuildTable objRef emptyTableMeta tableMeta
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
              let objRef = { schema = schema; name = objectName }
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

let private resolveSchema : SchemaName option -> SchemaName = function
    | None -> publicSchema
    | Some schema -> schema

let private migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    seq { for KeyValue (schemaName, schemaMeta) in toMeta.schemas do
              let resolvedName = resolveSchema schemaName
              match Map.tryFind schemaName fromMeta.schemas with
                  | None ->
                      yield SOCreateSchema resolvedName
                      yield! migrateBuildSchema schemaName emptySchemaMeta schemaMeta
                  | Some oldSchemaMeta -> yield! migrateBuildSchema schemaName oldSchemaMeta schemaMeta

          for KeyValue (schemaName, schemaMeta) in fromMeta.schemas do
              let resolvedName = resolveSchema schemaName
              if not (Map.containsKey schemaName toMeta.schemas) then
                  yield! deleteBuildSchema resolvedName schemaMeta
        }

// Order for database operations so that dependencies are not violated.
let private schemaOperationOrder = function
    | SODeleteConstraint (_, _) -> 0
    | SODeleteColumn _ -> 1
    | SODeleteTable _ -> 2
    | SODeleteSequence _ -> 3
    | SODeleteSchema _ -> 4
    | SOCreateSchema _ -> 5
    | SOCreateTable _ -> 6
    | SOCreateSequence _ -> 7
    | SOCreateColumn (_, _) -> 8
    | SOCreateConstraint (_, _, CMPrimaryKey _) -> 9
    | SOCreateConstraint (_, _, CMUnique _) -> 10
    | SOCreateConstraint (_, _, CMForeignKey (_, _)) -> 11
    | SOCreateConstraint (_, _, CMCheck _) -> 12

let migrateDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy schemaOperationOrder
