module FunWithFlags.FunDB.Layout.Correlate

open FunWithFlags.FunUtils

module FunQL = FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL

// We need to be defensive here; if we cannot correlate something, don't error out but just skip it.

let private correlateSchemaMeta (schemaKey : MigrationKey) (schema : SchemaMeta) : SchemaMeta =
    let mutable idColumns = Map.empty
    let metaObjects = schema.Objects

    let correlatePrimaryConstraint key ((objectName, object) as pair) =
        match object with
        | OMConstraint (tableName, CMPrimaryKey ([|idCol|], defer)) when not (Map.containsKey tableName idColumns) ->
            let newKey = sprintf "__correlated__primary__%O" tableName
            if Map.containsKey newKey metaObjects then
                (key, pair)
            else
                idColumns <- Map.add tableName idCol idColumns
                (newKey, pair)                
        | _ -> (key, pair)
    
    let metaObjects = metaObjects |> Map.mapWithKeysUnique correlatePrimaryConstraint

    let mutable idSequences = Map.empty

    let correlateTableObjects key (objectName, object) =
        match object with
        | OMTable tabl ->
            let tableColumns = tabl.Columns
            let tableColumns =
                match Map.tryFind objectName idColumns with
                | Some idName when not <| Map.containsKey "__correlated__id" tableColumns ->
                    let correlateIdColumn key (column : ColumnMeta) =
                        if column.Name = idName then
                            match column.ColumnType with
                            | CTPlain { DefaultExpr = Some (VEFunc (SQLName "nextval", [| VEValue (VRegclass { Schema = idSeqSchema; Name = idSeqName }) |])) }
                              when idSeqSchema = Some schema.Name || (schema.Name = SQLName "public" && idSeqSchema = None) ->
                                idSequences <- Map.add idSeqName objectName idSequences
                            | _ -> ()
                            ("__correlated__id", column)
                        else
                            (key, column)

                    tableColumns |> Map.mapWithKeysUnique correlateIdColumn
                | _ -> tableColumns
            (objectName, OMTable { tabl with Columns = tableColumns })
        | _ -> (objectName, object)

    let metaObjects = metaObjects |> Map.map correlateTableObjects

    let correlateSequence key ((objectName, object) as pair) : (MigrationKey * (SQLName * ObjectMeta)) =
        match object with
        | OMSequence ->
            match Map.tryFind objectName idSequences with
            | Some tableName ->
                let newKey = sprintf "__correlated__idseq__%O" tableName
                if Map.containsKey newKey metaObjects then
                    (key, pair)
                else
                    (newKey, pair)
            | _ -> (key, pair)
        | _ -> (key, pair)

    let metaObjects = metaObjects |> Map.mapWithKeysUnique correlateSequence
    { schema with Objects = metaObjects }

let correlateDatabaseMeta (meta : DatabaseMeta) : DatabaseMeta =
    { meta with Schemas = Map.map correlateSchemaMeta meta.Schemas }