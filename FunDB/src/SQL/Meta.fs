module internal FunWithFlags.FunDB.SQL.Meta

open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.Parser
open FunWithFlags.FunDB.SQL.Qualifier

let parseYesNo (str : string) =
    match str.ToLower() with
        | "yes" -> Some(true)
        | "no" -> Some(false)
        | _ -> None

let makeColumnMeta (dataType : string) (columnDefault : string) (isNullable : string) =
    match parseValueType dataType with
        | None -> failwith (sprintf "Unknown PostgreSQL value type: %s" dataType)
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
                              let vexpr = Parser.valueExpr Lexer.tokenstream lexbuf
                              Some(vexpr |> qualifyValueExpr |> localizeParsedValueExpr)
                    }

let makeTableFromName (schema : string) (table : string) : Table =
    { schema = Some(schema);
      name = table;
    }

let makeColumnFromName (schema : string) (table : string) (column : string) : Column =
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
    let systemSchemasValue = systemSchemas |> Set.toSeq |> Seq.map (fun x -> VEValue(VString(x))) |> Seq.toArray

    let stringQuery sel =
        let (_, values) = query.Query sel
        let getValue = function
            | VString(s) -> s
            | VNull -> ""
            | _ -> failwith "Unexpected non-string value"
        values |> Array.toSeq |> Seq.map (Array.map getValue)
    
    let schemasWhere = VENot(VEIn(VEColumn({ table = schemataTable; name = "schema_name"; }), systemSchemasValue))
    let schemasRes = stringQuery { simpleSelect [| "schema_name" |] schemataTable with
                                       where = Some(schemasWhere);
                                 }
    let schemas = schemasRes |> Seq.map (fun x -> x.[0]) |> Set.ofSeq
    let schemasValue = schemas |> Set.toSeq |> Seq.map (fun x -> VEValue(VString(x))) |> Seq.toArray

    let tablesWhere = VEIn(VEColumn({ table = tablesTable; name = "table_schema"; }), schemasValue)
    let tablesRes = stringQuery { simpleSelect [| "table_schema"; "table_name"; "table_type" |] tablesTable with
                                      where = Some(tablesWhere);
                                }
    let tableExtract (x : string array) =
        if x.[2] <> "BASE TABLE" then
            failwith "Unsupported table type"
        x.[1]
    let tables = tablesRes |> Seq.groupBy (fun x -> x.[0]) |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map tableExtract |> Set.ofSeq)) |> Map.ofSeq

    let columnsWhere = VEIn(VEColumn({ table = columnsTable; name = "table_schema"; }), schemasValue)
    let columnsRes = stringQuery { simpleSelect [| "table_schema"; "table_name"; "column_name"; "column_default"; "is_nullable"; "data_type" |] columnsTable with
                                       where = Some(columnsWhere);
                                 }
    let columns = columnsRes
                  |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                  |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> (LocalColumn(x.[2]), makeColumnMeta x.[5] x.[3] x.[4])) |> Map.ofSeq))
                  |> Map.ofSeq

    // NOTE: Constraint names are unique per schema.
    // NOTE: We assume that constraint_schema = table_schema always.
    // XXX: We skip CHECK constraints as we don't know how to handle them yet. We can do this via check_constraints table.
    let constraintsWhere = VEAnd(VEIn(VEColumn({ table = constraintsTable; name = "constraint_schema"; }), schemasValue),
                                 VENot(VEEq(VEColumn({ table = constraintsTable; name = "constraint_type"; }), VEValue(VString("CHECK")))))
    let constraintsRes = stringQuery { simpleSelect [| "constraint_schema"; "constraint_name"; "table_name"; "constraint_type" |] constraintsTable with
                                           where = Some(constraintsWhere);
                                     }
    let constraints = constraintsRes
                      |> Seq.groupBy (fun x -> x.[0])
                      |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> (x.[1], (x.[2], x.[3] |> parseConstraintType |> Option.get))) |> Map.ofSeq))
                      |> Map.ofSeq

    let keyColumnsWhere = VEIn(VEColumn({ table = keyColumnsTable; name = "constraint_schema"; }), schemasValue)
    let keyColumnsRes = stringQuery { simpleSelect [| "constraint_schema"; "constraint_name"; "column_name" |] keyColumnsTable with
                                          where = Some(keyColumnsWhere);
                                    }
    let keyColumns = keyColumnsRes
                     |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                     |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> LocalColumn(x.[2])) |> Set.ofSeq))
                     |> Map.ofSeq

    let refColumnsWhere = VEIn(VEColumn({ table = constraintColumnUsageTable; name = "constraint_schema"; }), schemasValue)
    let refColumnsRes = stringQuery { simpleSelect [| "constraint_schema"; "constraint_name"; "table_schema"; "table_name"; "column_name" |] constraintColumnUsageTable with
                                          where = Some(refColumnsWhere);
                                    }
    let refColumns = refColumnsRes
                     |> Seq.groupBy (fun x -> (x.[0], x.[1]))
                     |> Seq.map (fun (k, xs) -> (k, xs |> Seq.map (fun x -> makeColumnFromName x.[2] x.[3] x.[4]) |> Set.ofSeq))
                     |> Map.ofSeq

    let sequencesWhere = VEIn(VEColumn({ table = sequencesTable; name = "sequence_schema"; }), schemasValue)
    let sequencesRes = stringQuery { simpleSelect [| "sequence_schema"; "sequence_name" |] sequencesTable with
                                         where = Some(sequencesWhere);
                                   }
    let sequences = sequencesRes
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
