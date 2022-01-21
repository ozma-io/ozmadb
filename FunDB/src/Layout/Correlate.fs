module FunWithFlags.FunDB.Layout.Correlate

open FunWithFlags.FunUtils

module FunQL = FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL

let private correlateSchemaMeta (schemaName : SchemaName) (schemaKeys : MigrationKeysSet, schema : SchemaMeta) : MigrationKeysSet * SchemaMeta =
    let relations = schema.Relations

    let mutable idSequences = Map.empty

    let correlateTableObjects objectName object =
        match object with
        | OMTable tableObjects ->
            match tableObjects.Table with
            | None -> object
            | Some (tableKeys, table) ->
                let mutable idColumn = None

                let correlatePrimaryConstraint constrName (constrKeys, constr) =
                    match constr with
                    | CMPrimaryKey ([|idCol|], defer) ->
                        let newKey = sprintf "__primary__%O" objectName
                        idColumn <- Some idCol
                        (Set.add newKey constrKeys, constr)
                    | _ -> (constrKeys, constr)
                
                let constraints = Map.map correlatePrimaryConstraint tableObjects.Constraints

                let tableColumns = table.Columns
                let tableColumns =
                    match idColumn with
                    | None -> tableColumns
                    | Some idName ->
                        let correlateIdColumn name (keys, column : ColumnMeta) =
                            if name = idName then
                                match column.ColumnType with
                                | CTPlain { DefaultExpr = Some (VEFunc (SQLName "nextval", [| VEValue (VRegclass { Schema = idSeqSchema; Name = idSeqName }) |])) }
                                  when idSeqSchema = Some schemaName || (schemaName = SQLName "public" && idSeqSchema = None) ->
                                    idSequences <- Map.add idSeqName objectName idSequences
                                | _ -> ()
                                (Set.add "id" keys, column)
                            else
                                (keys, column)

                        tableColumns |> Map.map correlateIdColumn

                let table =
                    { table with
                        Columns = table.Columns
                    }
                let tableObjects =
                    { tableObjects with
                        Table = Some (tableKeys, table)
                        Constraints = constraints
                    }
                OMTable tableObjects
        | _ -> object

    let relations = relations |> Map.map correlateTableObjects

    let correlateSequence objectName object : RelationMeta =
        match object with
        | OMSequence keys ->
            match Map.tryFind objectName idSequences with
            | Some tableName ->
                let newKey = sprintf "__idseq__%O" tableName
                OMSequence (Set.add newKey keys)
            | _ -> object
        | _ -> object

    let relations = relations |> Map.map correlateSequence
    let ret = { schema with Relations = relations }
    (schemaKeys, ret)

let correlateDatabaseMeta (meta : DatabaseMeta) : DatabaseMeta =
    { meta with Schemas = Map.map correlateSchemaMeta meta.Schemas }