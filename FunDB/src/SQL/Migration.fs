module internal FunWithFlags.FunDB.SQL.Migration

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST

type MigrationPlan = SchemaOperation seq

let deleteBuildTable (table : Table) (tableMeta : TableMeta) : MigrationPlan =
    seq { for KeyValue(columnName, columnMeta) in tableMeta.columns do
              let obj = columnFromLocal table columnName
              yield SODeleteColumn(obj)
          yield SODeleteTable(table)
        }

let deleteBuildSchema (schemaName : string) (schemaMeta : SchemaMeta) : MigrationPlan =
    seq { for KeyValue(tableName, tableMeta) in schemaMeta.tables do
              let obj = { schema = Some(schemaName); name = tableName; }
              yield! deleteBuildTable obj tableMeta
          for sequenceName in schemaMeta.sequences do
              let obj = { schema = Some(schemaName); name = sequenceName; }
              yield SODeleteSequence(obj)
          for KeyValue(constraintName, (tableName, constraintMeta)) in schemaMeta.constraints do
              let obj = { schema = Some(schemaName); name = constraintName; }
              yield SODeleteConstraint(obj, tableName)
          yield SODeleteSchema(schemaName)
        }

let migrateBuildTable (table : Table) (fromMeta : TableMeta) (toMeta : TableMeta) : MigrationPlan =
    seq { for KeyValue(columnName, columnMeta) in toMeta.columns do
              let obj = columnFromLocal table columnName
              match Map.tryFind columnName fromMeta.columns with
                  | None -> yield SOCreateColumn(obj, columnMeta)
                  | Some(oldColumnMeta) ->
                      if oldColumnMeta <> columnMeta then
                          // XXX: Support altering columns instead of recreating them.
                          yield SODeleteColumn(obj)
                          yield SOCreateColumn(obj, columnMeta)

          for KeyValue(columnName, columnMeta) in fromMeta.columns do
              if not (Map.containsKey columnName toMeta.columns) then
                  let obj = columnFromLocal table columnName
                  yield SODeleteColumn(obj)
        }

let migrateBuildSchema (schemaName : string) (fromMeta : SchemaMeta) (toMeta : SchemaMeta) : MigrationPlan =
    seq { for KeyValue(tableName, tableMeta) in toMeta.tables do
              let obj = { schema = Some(schemaName); name = tableName; }
              match Map.tryFind tableName fromMeta.tables with
                  | None ->
                      yield SOCreateTable(obj)
                      yield! migrateBuildTable obj emptyTableMeta tableMeta
                  | Some(oldTableMeta) -> yield! migrateBuildTable obj oldTableMeta tableMeta
          for sequenceName in toMeta.sequences do
              let obj = { schema = Some(schemaName); name = sequenceName; }
              if not (Set.contains sequenceName fromMeta.sequences) then
                  yield SOCreateSequence(obj)
          for KeyValue(constraintName, ((tableName, constraintType) as constraintMeta)) in toMeta.constraints do
              let obj = { schema = Some(schemaName); name = constraintName; }
              match Map.tryFind constraintName fromMeta.constraints with
                  | None ->
                      yield SOCreateConstraint(obj, tableName, constraintType)
                  | Some(oldConstraintMeta) ->
                      if oldConstraintMeta <> constraintMeta then
                          yield SODeleteConstraint(obj, tableName)
                          yield SOCreateConstraint(obj, tableName, constraintType)

          for KeyValue(tableName, tableMeta) in fromMeta.tables do
              if not (Map.containsKey tableName toMeta.tables) then
                  let obj = { schema = Some(schemaName); name = tableName; }
                  yield! deleteBuildTable obj tableMeta
          for sequenceName in fromMeta.sequences do
              if not (Set.contains sequenceName toMeta.sequences) then
                  let obj = { schema = Some(schemaName); name = sequenceName; }
                  yield SODeleteSequence(obj)
          for KeyValue(constraintName, (tableName, constraintMeta)) in fromMeta.constraints do
              if not (Map.containsKey constraintName toMeta.constraints) then
                  let obj = { schema = Some(schemaName); name = constraintName; }
                  yield SODeleteConstraint(obj, tableName)
        }

let migrateBuildDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    seq { for KeyValue(schemaName, schemaMeta) in toMeta do
              match Map.tryFind schemaName fromMeta with
                  | None ->
                      yield SOCreateSchema(schemaName)
                      yield! migrateBuildSchema schemaName emptySchemaMeta schemaMeta
                  | Some(oldSchemaMeta) -> yield! migrateBuildSchema schemaName oldSchemaMeta schemaMeta

          for KeyValue(schemaName, schemaMeta) in fromMeta do
              if not (Map.containsKey schemaName toMeta) then
                  yield! deleteBuildSchema schemaName schemaMeta
        }

// Order for database operations so that dependencies are not violated.
let schemaOperationOrder = function
    | SODeleteConstraint(_, _) -> 0
    | SODeleteColumn(_) -> 1
    | SODeleteTable(_) -> 2
    | SODeleteSequence(_) -> 3
    | SODeleteSchema(_) -> 4
    | SOCreateSchema(_) -> 5
    | SOCreateTable(_) -> 6
    | SOCreateSequence(_) -> 7
    | SOCreateColumn(_, _) -> 8
    | SOCreateConstraint(_, _, CMPrimaryKey(_)) -> 9
    | SOCreateConstraint(_, _, CMUnique(_)) -> 10
    | SOCreateConstraint(_, _, CMForeignKey(_, _)) -> 11

let migrateDatabase (fromMeta : DatabaseMeta) (toMeta : DatabaseMeta) : MigrationPlan =
    migrateBuildDatabase fromMeta toMeta |> Seq.sortBy schemaOperationOrder
