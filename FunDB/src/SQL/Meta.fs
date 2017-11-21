module internal FunWithFlags.FunDB.SQL.Meta

open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Parse
// XXX: We use FunQL parser to parse value expressions
open FunWithFlags.FunDB.FunQL

let rec localizeDefaultExpr = function
    | WValue(v) -> WValue(v)
    | WColumn(_) -> failwith "Column references are not supported in default values"
    | WNot(a) -> WNot(localizeDefaultExpr a)
    | WConcat(a, b) -> WConcat(localizeDefaultExpr a, localizeDefaultExpr b)
    | WEq(a, b) -> WEq(localizeDefaultExpr a, localizeDefaultExpr b)
    | WIn(a, b) -> WIn(localizeDefaultExpr a, localizeDefaultExpr b)
    | WAnd(a, b) -> WAnd(localizeDefaultExpr a, localizeDefaultExpr b)
    | WFunc(name, args) -> WFunc(name, Array.map localizeDefaultExpr args)
    | WCast(a, typ) -> WCast(localizeDefaultExpr a, typ)

let makeColumnMeta (dataType : string) (columnDefault : string) (isNullable : string) =
    match parseValueType dataType with
        | None -> failwith "Unknown PostgreSQL value type"
        | Some(typ) ->
            match parseYesNo isNullable with
                | None -> failwith "Invalid PostgreSQL YES/NO value"
                | Some(nullable) ->
                    { colType = typ;
                      nullable = nullable;
                      defaultValue =
                          if columnDefault = "" then
                              None
                          else
                              let lexbuf = LexBuffer<char>.FromString columnDefault
                              Some(Parser.valueExpr Lexer.tokenstream lexbuf |> localizeDefaultExpr)
                    }

let makeTableFromName (schema : string) (table : string) =
    { schema = Some(schema);
      name = table;
    }

let makeColumnFromName (schema : string) (table : string) (column : string) =
    { table = makeTableFromName schema table;
      name = column;
    }

let getDatabaseMeta (query : QueryConnection) : DatabaseMeta =
    // Get schemas. Ignore system ones.
    let infoTable name = { schema = Some("information_schema"); name = name; }
    let schemataTable = infoTable "schemata"
    let tablesTable = infoTable "tables"
    let columnsTable = infoTable "columns"
    let constraintsTable = infoTable "table_constraints"
    let keyColumnsTable = infoTable "key_column_usage"
    let constraintColumnUsageTable = infoTable "constraint_column_usage"
    let sequencesTable = infoTable "sequences"
    let systemSchemas = set [| "pg_catalog"; "public"; "information_schema" |]
    let systemSchemasValue = WValue(VArray(systemSchemas |> Set.toSeq |> Seq.map VString |> Seq.toArray))
    
    let schemasWhere = WNot(WIn(WColumn({ table = schemataTable; name = "schema_name"; }), systemSchemasValue))
    let schemasRes = query.Query { simpleSelect [| "schema_name" |] schemataTable with
                                       where = Some(schemasWhere);
                                 }
    let schemas = schemasRes |> Array.toSeq |> Seq.map (fun x -> x.[0]) |> Set.ofSeq
    let schemasValue = WValue(VArray(schemas |> Set.toSeq |> Seq.map VString |> Seq.toArray))

    let tablesWhere = WIn(WColumn({ table = tablesTable; name = "table_schema"; }), schemasValue)
    let tablesRes = query.Query { simpleSelect [| "table_schema"; "table_name"; "table_type" |] tablesTable with
                                      where = Some(tablesWhere);
                                }
    let tableExtract (x : string array) =
        if x.[2] <> "BASE TABLE" then
            failwith "Unsupported table type"
        x.[1]
    let tables = tablesRes |> Array.toSeq |> Seq.groupBy (fun x -> x.[0]) |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map tableExtract |> Set.ofSeq)) |> Map.ofSeq

    let columnsWhere = WIn(WColumn({ table = columnsTable; name = "table_schema"; }), schemasValue)
    let columnsRes = query.Query { simpleSelect [| "table_schema"; "table_name"; "column_name"; "column_default"; "is_nullable"; "data_type" |] columnsTable with
                                       where = Some(columnsWhere);
                                 }
    let columns = columnsRes |> Array.toSeq
                  |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                  |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> (LocalColumn(x.[2]), makeColumnMeta x.[5] x.[3] x.[4])) |> Map.ofSeq))
                  |> Map.ofSeq

    // NOTE: Constraint names are unique per schema.
    // NOTE: We assume that constraint_schema = table_schema always.
    let constraintsWhere = WIn(WColumn({ table = constraintsTable; name = "constraint_schema"; }), schemasValue)
    let constraintsRes = query.Query { simpleSelect [| "constraint_schema"; "constraint_name"; "table_name"; "constraint_type" |] constraintsTable with
                                           where = Some(constraintsWhere);
                                     }
    let constraints = constraintsRes |> Array.toSeq
                      |> Seq.groupBy (fun x -> x.[0])
                      |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> (x.[1], (x.[2], x.[3] |> parseConstraintType |> Option.get))) |> Map.ofSeq))
                      |> Map.ofSeq

    let keyColumnsWhere = WIn(WColumn({ table = keyColumnsTable; name = "constraint_schema"; }), schemasValue)
    let keyColumnsRes = query.Query { simpleSelect [| "constraint_schema"; "constraint_name"; "column_name" |] keyColumnsTable with
                                          where = Some(keyColumnsWhere);
                                    }
    let keyColumns = keyColumnsRes |> Array.toSeq
                     |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                     |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> LocalColumn(x.[2])) |> Set.ofSeq))
                     |> Map.ofSeq

    let refColumnsWhere = WIn(WColumn({ table = constraintColumnUsageTable; name = "constraint_schema"; }), schemasValue)
    let refColumnsRes = query.Query { simpleSelect [| "constraint_schema"; "constraint_name"; "table_schema"; "table_name"; "column_name" |] constraintColumnUsageTable with
                                          where = Some(refColumnsWhere);
                                    }
    let refColumns = refColumnsRes |> Array.toSeq
                     |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                     |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> makeColumnFromName x.[2] x.[3] x.[4]) |> Set.ofSeq))
                     |> Map.ofSeq

    let sequencesWhere = WIn(WColumn({ table = sequencesTable; name = "sequence_schema"; }), schemasValue)
    let sequencesRes = query.Query { simpleSelect [| "sequence_schema"; "sequence_name" |] sequencesTable with
                                         where = Some(sequencesWhere);
                                   }
    let sequences = sequencesRes |> Array.toSeq
                    |> Seq.groupBy (fun x -> (x.[0]))
                    |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> x.[1]) |> Set.ofSeq))
                    |> Map.ofSeq

    // Finally join this all.
    let makeConstraintMeta (schema : string) (cname : string) (ctype : ConstraintType) : ConstraintMeta =
        match ctype with
            | CTUnique -> CMUnique(keyColumns.[(schema, cname)])
            | CTPrimaryKey -> CMPrimaryKey(keyColumns.[(schema, cname)])
            | CTForeignKey -> CMForeignKey(setHead keyColumns.[(schema, cname)], setHead refColumns.[(schema, cname)])
    let makeTableMeta (schema : string) (table : string) : TableMeta =
        { columns = mapGetWithDefault (schema, table) Map.empty columns;
        }
    let makeSchemaMeta (schema : string) : SchemaMeta =
        { tables = mapGetWithDefault schema Set.empty tables |> setToMap (makeTableMeta schema);
          constraints = mapGetWithDefault schema Map.empty constraints |> Map.map (fun cname (tname, ctype) -> (tname, makeConstraintMeta schema cname ctype));
          sequences = mapGetWithDefault schema Set.empty sequences;
        }

    setToMap (fun schema -> makeSchemaMeta schema) schemas
