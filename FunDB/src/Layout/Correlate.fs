module FunWithFlags.FunDB.Layout.Correlate

open FunWithFlags.FunUtils

module FunQL = FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL

let private correlateSchemaMeta (schemaName : SchemaName) (schemaKeys : MigrationKeysSet, schema : SchemaMeta) : MigrationKeysSet * SchemaMeta =
    let mutable idColumns = Map.empty
    let metaObjects = schema.Objects

    let correlatePrimaryConstraint objectName ((keys, object) as pair) =
        match object with
        | OMConstraint (tableName, CMPrimaryKey ([|idCol|], defer)) when not (Map.containsKey tableName idColumns) ->
            let newKey = sprintf "__primary__%O" tableName
            idColumns <- Map.add tableName idCol idColumns
            (Set.add newKey keys, object)
        | _ -> pair
    
    let metaObjects = metaObjects |> Map.map correlatePrimaryConstraint

    let mutable idSequences = Map.empty

    let correlateTableObjects objectName (keys, object) =
        match object with
        | OMTable tabl ->
            let tableColumns = tabl.Columns
            let tableColumns =
                match Map.tryFind objectName idColumns with
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
                | _ -> tableColumns
            (keys, OMTable { tabl with Columns = tableColumns })
        | _ -> (keys, object)

    let metaObjects = metaObjects |> Map.map correlateTableObjects

    let correlateSequence objectName ((keys, object) as pair) : MigrationKeysSet * ObjectMeta =
        match object with
        | OMSequence ->
            match Map.tryFind objectName idSequences with
            | Some tableName ->
                let newKey = sprintf "__idseq__%O" tableName
                (Set.add newKey keys, object)
            | _ -> pair
        | _ -> pair

    let metaObjects = metaObjects |> Map.map correlateSequence
    let ret = { schema with Objects = metaObjects }
    (schemaKeys, ret)

let correlateDatabaseMeta (meta : DatabaseMeta) : DatabaseMeta =
    { meta with Schemas = Map.map correlateSchemaMeta meta.Schemas }