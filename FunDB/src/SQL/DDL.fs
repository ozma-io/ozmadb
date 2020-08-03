module FunWithFlags.FunDB.SQL.DDL

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST

type ConstraintName = SQLName
type IndexName = SQLName
type SequenceName = SQLName
type TriggerName = SQLName

[<NoEquality; NoComparison>]
type ColumnMeta =
    { columnType : DBValueType
      isNullable : bool
      defaultExpr : ValueExpr option
    }

type ConstraintType =
    | CTUnique
    | CTCheck
    | CTPrimaryKey
    | CTForeignKey

[<StructuralEquality; NoComparison>]
type ConstraintMeta =
    | CMUnique of ColumnName[]
    | CMCheck of ValueExpr
    | CMPrimaryKey of ColumnName[]
    | CMForeignKey of TableRef * (ColumnName * ColumnName)[]

[<StructuralEquality; NoComparison>]
type IndexMeta =
    { columns : ColumnName[]
    }

[<NoEquality; NoComparison>]
type TableMeta =
    { columns : Map<ColumnName, ColumnMeta>
    }

let emptyTableMeta =
    { columns = Map.empty
    }

let unionTableMeta (a : TableMeta) (b : TableMeta) =
    { columns = Map.unionUnique a.columns b.columns
    }

type FunctionMode =
    | FMIn
    | FMOut
    | FMInOut
    | FMVariadic
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FMIn -> "IN"
            | FMOut -> "OUT"
            | FMInOut -> "INOUT"
            | FMVariadic -> "VARIADIC"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type FunctionArgument =
    { name : SQLName option
      defaultValue : ValueExpr option // Do not set to NULL
    }

type FunctionArgumentSignature =
    { typeName : TypeName
      mode : FunctionMode
    }

let private functionSignatureToString (signature : FunctionArgumentSignature) =
    sprintf "%s %s" (signature.mode.ToSQLString()) (signature.typeName.ToSQLString())

let private functionArgumentToString (signature : FunctionArgumentSignature) (arg : FunctionArgument) =
    let nameStr =
        match arg.name with
        | None -> ""
        | Some n -> n.ToSQLString()
    let defaultStr =
        match arg.defaultValue with
        | None -> ""
        | Some d -> sprintf "= %s" (d.ToSQLString())
    concatWithWhitespaces [signature.mode.ToSQLString(); nameStr; signature.typeName.ToSQLString(); defaultStr]

[<StructuralEquality; NoComparison>]
type FunctionReturn =
    | FRTable of (TypeName * ColumnName)[]
    | FRValue of TypeName
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FRTable cols ->
                cols
                    |> Seq.map (fun (typ, name) -> sprintf "%s %s" (name.ToSQLString()) (typ.ToSQLString()))
                    |> String.concat ", "
                    |> sprintf "TABLE (%s)"
            | FRValue typ -> typ.ToSQLString()

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type FunctionBehaviour =
    | FBImmutable
    | FBStable
    | FBVolatile
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | FBImmutable -> "IMMUTABLE"
            | FBStable -> "STABLE"
            | FBVolatile -> "VOLATILE"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type FunctionDefinition =
    { arguments : FunctionArgument[]
      returnValue : FunctionReturn
      behaviour : FunctionBehaviour
      language : SQLName
      definition : string
    }

type TriggerOrder =
    | TOBefore
    | TOAfter
    | TOInsteadOf
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TOBefore -> "BEFORE"
            | TOAfter -> "AFTER"
            | TOInsteadOf -> "INSTEAD OF"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type TriggerEvent =
    | TEInsert
    | TEUpdate of ColumnName[] option
    | TEDelete
    | TETruncate
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TEInsert -> "INSERT"
            | TEUpdate None -> "UPDATE"
            | TEUpdate (Some cols) ->
                assert (not <| Array.isEmpty cols)
                cols |> Seq.map (fun c -> c.ToSQLString()) |> String.concat ", " |> sprintf "UPDATE OF %s"
            | TEDelete -> "DELETE"
            | TETruncate -> "TRUNCATE"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type TriggerMode =
    | TMEachRow
    | TMEachStatement
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TMEachRow -> "FOR EACH ROW"
            | TMEachStatement -> "FOR EACH STATEMENT"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type TriggerDefinition =
    { isConstraint : bool
      order : TriggerOrder
      events : TriggerEvent[]
      mode : TriggerMode
      condition : ValueExpr option
      functionName : SchemaObject
      functionArgs : string[]
    }

type FunctionSignature = ComparableArray<FunctionArgumentSignature>

[<NoEquality; NoComparison>]
type ObjectMeta =
    | OMTable of TableMeta
    | OMSequence
    | OMConstraint of TableName * ConstraintMeta
    | OMIndex of TableName * IndexMeta
    | OMFunction of Map<FunctionSignature, FunctionDefinition>
    | OMTrigger of TableName * TriggerDefinition

[<NoEquality; NoComparison>]
type SchemaMeta =
    { objects : Map<SQLName, ObjectMeta>
    }

let emptySchemaMeta =
    { objects = Map.empty
    }

let unionSchemaMeta (a : SchemaMeta) (b : SchemaMeta) =
    { objects = Map.unionUnique a.objects b.objects
    }

[<NoEquality; NoComparison>]
type DatabaseMeta =
    { schemas : Map<SchemaName, SchemaMeta>
    }

let emptyDatabaseMeta =
    { schemas = Map.empty
    }

let unionDatabaseMeta (a : DatabaseMeta) (b : DatabaseMeta) =
    { schemas = Map.unionWith (fun name -> unionSchemaMeta) a.schemas b.schemas
    }

[<NoEquality; NoComparison>]
type TableOperation =
    | TOCreateColumn of ColumnName * ColumnMeta
    | TODeleteColumn of ColumnName
    | TOAlterColumnType of ColumnName * DBValueType
    | TOAlterColumnNull of ColumnName * bool
    | TOAlterColumnDefault of ColumnName * ValueExpr option
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TOCreateColumn (col, pars) ->
                let notNullStr = if pars.isNullable then "NULL" else "NOT NULL"
                let defaultStr =
                    match pars.defaultExpr with
                    | None -> ""
                    | Some def -> sprintf "DEFAULT %s" (def.ToSQLString())
                sprintf "ADD COLUMN %s %s %s %s" (col.ToSQLString()) (pars.columnType.ToSQLString()) notNullStr defaultStr
            | TODeleteColumn col -> sprintf "DROP COLUMN %s" (col.ToSQLString())
            | TOAlterColumnType (col, typ) -> sprintf "ALTER COLUMN %s SET DATA TYPE %s" (col.ToSQLString()) (typ.ToSQLString())
            | TOAlterColumnNull (col, isNullable) -> sprintf "ALTER COLUMN %s %s NOT NULL" (col.ToSQLString()) (if isNullable then "DROP" else "SET")
            | TOAlterColumnDefault (col, None) -> sprintf "ALTER COLUMN %s DROP DEFAULT" (col.ToSQLString())
            | TOAlterColumnDefault (col, Some def) -> sprintf "ALTER COLUMN %s SET DEFAULT %s" (col.ToSQLString()) (def.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type SchemaOperation =
    | SOCreateSchema of SchemaName
    | SODeleteSchema of SchemaName
    | SOCreateTable of TableRef
    | SODeleteTable of TableRef
    | SOCreateSequence of SchemaObject
    | SODeleteSequence of SchemaObject
    // Constraint operations are not plain ALTER TABLE operations because they create new objects at schema namespace.
    | SOCreateConstraint of SchemaObject * TableName * ConstraintMeta
    | SODeleteConstraint of SchemaObject * TableName
    | SOCreateIndex of SchemaObject * TableName * IndexMeta
    | SODeleteIndex of SchemaObject
    | SOAlterTable of TableRef * TableOperation[]
    | SOCreateOrReplaceFunction of SchemaObject * FunctionSignature * FunctionDefinition
    | SODropFunction of SchemaObject * FunctionSignature
    | SOCreateTrigger of SchemaObject * TableName * TriggerDefinition
    | SODropTrigger of SchemaObject * TableName
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SOCreateSchema schema -> sprintf "CREATE SCHEMA %s" (schema.ToSQLString())
            | SODeleteSchema schema -> sprintf "DROP SCHEMA %s" (schema.ToSQLString())
            | SOCreateTable table -> sprintf "CREATE TABLE %s ()" (table.ToSQLString())
            | SODeleteTable table -> sprintf "DROP TABLE %s" (table.ToSQLString())
            | SOCreateSequence seq -> sprintf "CREATE SEQUENCE %s" (seq.ToSQLString())
            | SODeleteSequence seq -> sprintf "DROP SEQUENCE %s" (seq.ToSQLString())
            | SOCreateConstraint (constr, table, pars) ->
                let constraintStr =
                    match pars with
                    | CMUnique exprs ->
                        assert (not <| Array.isEmpty exprs)
                        exprs |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "UNIQUE (%s)"
                    | CMPrimaryKey cols ->
                        assert (not <| Array.isEmpty cols)
                        cols |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "PRIMARY KEY (%s)"
                    | CMForeignKey (ref, cols) ->
                        let myCols = cols |> Seq.map (fun (name, refName) -> name.ToSQLString()) |> String.concat ", "
                        let refCols = cols |> Seq.map (fun (name, refName) -> refName.ToSQLString()) |> String.concat ", "
                        sprintf "FOREIGN KEY (%s) REFERENCES %s (%s)" myCols (ref.ToSQLString()) refCols
                    | CMCheck expr -> sprintf "CHECK (%s)" (expr.ToSQLString())
                sprintf "ALTER TABLE %s ADD CONSTRAINT %s %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString()) constraintStr
            | SODeleteConstraint (constr, table) -> sprintf "ALTER TABLE %s DROP CONSTRAINT %s" ({ constr with name = table }.ToSQLString()) (constr.name.ToSQLString())
            | SOCreateIndex (index, table, pars) ->
                let cols =
                    assert (not <| Array.isEmpty pars.columns)
                    pars.columns |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", "
                sprintf "CREATE INDEX %s ON %s (%s)" (index.name.ToSQLString()) ({ index with name = table }.ToSQLString()) cols
            | SODeleteIndex index -> sprintf "DROP INDEX %s" (index.ToSQLString())
            | SOAlterTable (table, ops) -> sprintf "ALTER TABLE %s %s" (table.ToSQLString()) (ops |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", ")
            | SOCreateOrReplaceFunction (func, args, def) ->
                assert (Array.length args.items = Array.length def.arguments)
                let argsStr = Seq.map2 functionArgumentToString args def.arguments |> String.concat ", "
                sprintf "CREATE OR REPLACE FUNCTION %s (%s) RETURNS %s LANGUAGE %s %s AS %s"
                    (func.ToSQLString())
                    argsStr
                    (def.returnValue.ToSQLString())
                    (def.language.ToSQLString())
                    (def.behaviour.ToSQLString())
                    (renderSqlString def.definition)
            | SODropFunction (func, args) ->
                let argsStr = args.items |> Seq.map functionSignatureToString |> String.concat ", "
                sprintf "DROP FUNCTION %s (%s)" (func.ToSQLString()) argsStr
            | SOCreateTrigger (trigger, table, def) ->
                assert (not <| Array.isEmpty def.events)
                let constraintStr = if def.isConstraint then "CONSTRAINT" else ""
                let eventsStr = def.events |> Seq.map (fun e -> e.ToSQLString()) |> String.concat " OR "
                let triggerStr =
                    sprintf "TRIGGER %s %s %s ON %s %s"
                        (trigger.name.ToSQLString())
                        (def.order.ToSQLString())
                        eventsStr
                        ({ schema = trigger.schema; name = table }.ToSQLString())
                        (def.mode.ToSQLString())
                let whenStr =
                    match def.condition with
                    | None -> ""
                    | Some cond -> sprintf "WHEN (%s)" (cond.ToSQLString())
                let argsStr = def.functionArgs |> Seq.map renderSqlString |> String.concat ", "
                let tailStr = sprintf "EXECUTE PROCEDURE %s (%s)" (def.functionName.ToSQLString()) argsStr
                concatWithWhitespaces ["CREATE"; constraintStr; triggerStr; whenStr; tailStr]
            | SODropTrigger (trigger, table) ->
                sprintf "DROP TRIGGER %s ON %s" (trigger.name.ToSQLString()) ({ schema = trigger.schema; name = table }.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()