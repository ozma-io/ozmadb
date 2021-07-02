module FunWithFlags.FunDB.SQL.DDL

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST

type ConstraintName = SQLName
type IndexName = SQLName
type SequenceName = SQLName
type TriggerName = SQLName
type ExtensionName = SQLName
type OpClassName = SQLName
type AccessMethodName = SQLName
type MigrationKey = string
type MigrationKeysSet = Set<MigrationKey>
type MigrationObjectsMap<'v> = Map<SQLName, MigrationKeysSet * 'v>

[<NoEquality; NoComparison>]
type PlainColumnMeta =
    { DefaultExpr : ValueExpr option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let defaultStr =
                match this.DefaultExpr with
                | None -> ""
                | Some def -> sprintf "DEFAULT %s" (def.ToSQLString())
            defaultStr

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type ColumnType =
    | CTPlain of PlainColumnMeta
    | CTGeneratedStored of ValueExpr
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | CTPlain plain -> plain.ToSQLString()
            | CTGeneratedStored expr -> sprintf "GENERATED ALWAYS AS (%s) STORED" (expr.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type ColumnMeta =
    { DataType : DBValueType
      ColumnType : ColumnType
      IsNullable : bool
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let notNullStr = if this.IsNullable then "NULL" else "NOT NULL"
            sprintf "%s %s %s" (this.DataType.ToSQLString()) notNullStr (this.ColumnType.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

type ConstraintType =
    | CTUnique
    | CTCheck
    | CTPrimaryKey
    | CTForeignKey

[<StructuralEquality; NoComparison>]
type DeferrableConstraint =
    | DCNotDeferrable
    | DCDeferrable of InitiallyDeferred : bool
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | DCNotDeferrable -> "NOT DEFERRABLE"
            | DCDeferrable initiallyDef ->
                let initiallyDeferredStr =
                    if initiallyDef then "INITIALLY DEFERRED" else "INITIALLY IMMEDIATE"
                sprintf "DEFERRABLE %s" initiallyDeferredStr

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type ConstraintMeta =
    | CMUnique of ColumnName[] * DeferrableConstraint
    | CMCheck of StringComparable<ValueExpr>
    | CMPrimaryKey of ColumnName[] * DeferrableConstraint
    | CMForeignKey of TableRef * (ColumnName * ColumnName)[] * DeferrableConstraint
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | CMUnique (exprs, defer) ->
                assert (not <| Array.isEmpty exprs)
                let exprsStr = exprs |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", "
                sprintf "UNIQUE (%s) %s" exprsStr (defer.ToSQLString())
            | CMPrimaryKey (cols, defer) ->
                assert (not <| Array.isEmpty cols)
                let colsStr = cols |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", "
                sprintf "PRIMARY KEY (%s) %s" colsStr (defer.ToSQLString())
            | CMForeignKey (ref, cols, defer) ->
                let myCols = cols |> Seq.map (fun (name, refName) -> name.ToSQLString()) |> String.concat ", "
                let refCols = cols |> Seq.map (fun (name, refName) -> refName.ToSQLString()) |> String.concat ", "
                sprintf "FOREIGN KEY (%s) REFERENCES %s (%s) %s" myCols (ref.ToSQLString()) refCols (defer.ToSQLString())
            | CMCheck expr -> sprintf "CHECK (%s)" (expr.ToString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type IndexKey =
    | IKColumn of ColumnName
    | IKExpression of StringComparable<ValueExpr>
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IKColumn col -> col.ToSQLString()
            | IKExpression expr -> sprintf "(%O)" expr

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type IndexColumn =
    { Key : IndexKey
      OpClass : OpClassName option
      Order : SortOrder option
      Nulls : NullsOrder option
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let opClassStr = optionToSQLString this.OpClass
            let orderStr = optionToSQLString this.Order
            let nullsStr = optionToSQLString this.Nulls
            String.concatWithWhitespaces [this.Key.ToSQLString(); opClassStr; orderStr; nullsStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<StructuralEquality; NoComparison>]
type IndexMeta =
    { Columns : IndexColumn[]
      IncludedColumns : IndexKey[]
      IsUnique : bool
      AccessMethod : AccessMethodName
      Predicate : StringComparable<ValueExpr> option
    }

[<NoEquality; NoComparison>]
type TableMeta =
    { Columns : MigrationObjectsMap<ColumnMeta>
    }

let emptyTableMeta =
    { Columns = Map.empty
    }

let unionTableMeta (a : TableMeta) (b : TableMeta) =
    { Columns = Map.unionUnique a.Columns b.Columns
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
    { Name : SQLName option
      DefaultValue : StringComparable<ValueExpr> option // Do not set to NULL
    }

type FunctionArgumentSignature =
    { TypeName : TypeName
      Mode : FunctionMode
    }

let private functionSignatureToString (signature : FunctionArgumentSignature) =
    sprintf "%s %s" (signature.Mode.ToSQLString()) (signature.TypeName.ToSQLString())

let private functionArgumentToString (signature : FunctionArgumentSignature) (arg : FunctionArgument) =
    let nameStr = optionToSQLString arg.Name
    let defaultStr =
        match arg.DefaultValue with
        | None -> ""
        | Some d -> sprintf "= %s" (d.Value.ToSQLString())
    String.concatWithWhitespaces [signature.Mode.ToSQLString(); nameStr; signature.TypeName.ToSQLString(); defaultStr]

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
    { Arguments : FunctionArgument[]
      ReturnValue : FunctionReturn
      Behaviour : FunctionBehaviour
      Language : SQLName
      Definition : string
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
    { IsConstraint : DeferrableConstraint option
      Order : TriggerOrder
      Events : TriggerEvent[]
      Mode : TriggerMode
      Condition : StringComparable<ValueExpr> option
      FunctionName : SchemaObject
      FunctionArgs : string[]
    }

type FunctionSignature = FunctionArgumentSignature[]

[<NoEquality; NoComparison>]
type ObjectMeta =
    | OMTable of TableMeta
    | OMSequence
    | OMConstraint of TableName * ConstraintMeta
    | OMIndex of TableName * IndexMeta
    | OMFunction of Map<FunctionSignature, FunctionDefinition>
    | OMTrigger of TableName * TriggerDefinition

type SchemaObjects = MigrationObjectsMap<ObjectMeta>

[<NoEquality; NoComparison>]
type SchemaMeta =
    { Objects : SchemaObjects
    }

let unionSchemaMeta (a : SchemaMeta) (b : SchemaMeta) =
    { Objects = Map.unionUnique a.Objects b.Objects
    }

[<NoEquality; NoComparison>]
type DatabaseMeta =
    { Schemas : MigrationObjectsMap<SchemaMeta>
      Extensions : Set<ExtensionName>
    }

let emptyDatabaseMeta =
    { Schemas = Map.empty
      Extensions = Set.empty
    }

let unionDatabaseMeta (a : DatabaseMeta) (b : DatabaseMeta) =
    { Schemas = Map.unionWith (fun key (keys1, meta1) (keys2, meta2) -> (Set.union keys1 keys2, unionSchemaMeta meta1 meta2)) a.Schemas b.Schemas
      Extensions = Set.union a.Extensions b.Extensions
    }

let filterDatabaseMeta (f : SchemaName -> bool) (meta : DatabaseMeta) =
    { Schemas = Map.filter (fun name (keys, meta) -> f name) meta.Schemas
      Extensions = meta.Extensions
    }

[<NoEquality; NoComparison>]
type TableOperation =
    | TOCreateColumn of ColumnName * ColumnMeta
    | TODropColumn of ColumnName
    | TOAlterColumnType of ColumnName * DBValueType
    | TOAlterColumnNull of ColumnName * bool
    | TOAlterColumnDefault of ColumnName * ValueExpr option
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | TOCreateColumn (name, pars) -> sprintf "ADD COLUMN %s %s" (name.ToSQLString()) (pars.ToSQLString())
            | TODropColumn col -> sprintf "DROP COLUMN %s" (col.ToSQLString())
            | TOAlterColumnType (col, typ) -> sprintf "ALTER COLUMN %s SET DATA TYPE %s" (col.ToSQLString()) (typ.ToSQLString())
            | TOAlterColumnNull (col, isNullable) -> sprintf "ALTER COLUMN %s %s NOT NULL" (col.ToSQLString()) (if isNullable then "DROP" else "SET")
            | TOAlterColumnDefault (col, None) -> sprintf "ALTER COLUMN %s DROP DEFAULT" (col.ToSQLString())
            | TOAlterColumnDefault (col, Some def) -> sprintf "ALTER COLUMN %s SET DEFAULT %s" (col.ToSQLString()) (def.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type SchemaOperation =
    | SOCreateExtension of ExtensionName
    | SODropExtension of ExtensionName
    | SOCreateSchema of SchemaName
    | SORenameSchema of SchemaName * SchemaName
    | SODropSchema of SchemaName
    | SOCreateTable of TableRef
    | SORenameTable of TableRef * TableName
    | SOAlterTable of TableRef * TableOperation[]
    | SORenameTableColumn of TableRef * ColumnName * ColumnName
    | SODropTable of TableRef
    | SOCreateSequence of SchemaObject
    | SORenameSequence of SchemaObject * SQLName
    | SODropSequence of SchemaObject
    | SOCreateConstraint of SchemaObject * TableName * ConstraintMeta
    | SORenameConstraint of SchemaObject * TableName * ConstraintName
    | SOAlterConstraint of SchemaObject * TableName * DeferrableConstraint
    | SODropConstraint of SchemaObject * TableName
    | SOCreateIndex of SchemaObject * TableName * IndexMeta
    | SORenameIndex of SchemaObject * IndexName
    | SODropIndex of SchemaObject
    | SOCreateOrReplaceFunction of SchemaObject * FunctionSignature * FunctionDefinition
    | SORenameFunction of SchemaObject * FunctionSignature * FunctionName
    | SODropFunction of SchemaObject * FunctionSignature
    | SOCreateTrigger of SchemaObject * TableName * TriggerDefinition
    | SORenameTrigger of SchemaObject * TableName * TriggerName
    | SODropTrigger of SchemaObject * TableName
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | SOCreateExtension name -> sprintf "CREATE EXTENSION %s" (name.ToSQLString())
            | SODropExtension name -> sprintf "DROP EXTENSION %s" (name.ToSQLString())
            | SOCreateSchema schema -> sprintf "CREATE SCHEMA %s" (schema.ToSQLString())
            | SORenameSchema (schema, toName) -> sprintf "ALTER SCHEMA %s RENAME TO %s" (schema.ToSQLString()) (toName.ToSQLString())
            | SODropSchema schema -> sprintf "DROP SCHEMA %s" (schema.ToSQLString())
            | SOCreateTable table -> sprintf "CREATE TABLE %s ()" (table.ToSQLString())
            | SORenameTable (table, toName) -> sprintf "ALTER TABLE %s RENAME TO %s" (table.ToSQLString()) (toName.ToSQLString())
            | SOAlterTable (table, ops) -> sprintf "ALTER TABLE %s %s" (table.ToSQLString()) (ops |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", ")
            | SORenameTableColumn (table, col, toCol) -> sprintf "ALTER TABLE %s RENAME COLUMN %s TO %s" (table.ToSQLString()) (col.ToSQLString()) (toCol.ToSQLString())
            | SODropTable table -> sprintf "DROP TABLE %s" (table.ToSQLString())
            | SOCreateSequence seq -> sprintf "CREATE SEQUENCE %s" (seq.ToSQLString())
            | SORenameSequence (seq, toName) -> sprintf "ALTER SEQUENCE %s RENAME TO %s" (seq.ToSQLString()) (toName.ToSQLString())
            | SODropSequence seq -> sprintf "DROP SEQUENCE %s" (seq.ToSQLString())
            | SOCreateConstraint (constr, table, pars) ->
                sprintf "ALTER TABLE %s ADD CONSTRAINT %s %s" ({ constr with Name = table }.ToSQLString()) (constr.Name.ToSQLString()) (pars.ToSQLString())
            | SORenameConstraint (constr, table, toName) -> sprintf "ALTER TABLE %s RENAME CONSTRAINT %s TO %s" ({ constr with Name = table }.ToSQLString()) (constr.Name.ToSQLString()) (toName.ToSQLString())
            | SOAlterConstraint (constr, table, alter) ->
                let initStr = sprintf "ALTER TABLE %s ALTER CONSTRAINT %s" ({ constr with Name = table }.ToSQLString()) (constr.Name.ToSQLString())
                let alterStr = alter.ToSQLString()
                String.concatWithWhitespaces [initStr; alterStr]
            | SODropConstraint (constr, table) -> sprintf "ALTER TABLE %s DROP CONSTRAINT %s" ({ constr with Name = table }.ToSQLString()) (constr.Name.ToSQLString())
            | SOCreateIndex (index, table, pars) ->
                let columnsStr =
                    assert (not <| Array.isEmpty pars.Columns)
                    pars.Columns |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", "
                let uniqueStr = if pars.IsUnique then "UNIQUE" else ""
                let suffixStr = sprintf "INDEX %s ON %s USING %s (%s)" (index.Name.ToSQLString()) ({ index with Name = table }.ToSQLString()) (pars.AccessMethod.ToSQLString()) columnsStr
                let includeStr =
                    if Array.isEmpty pars.IncludedColumns then
                        ""
                    else
                        pars.IncludedColumns |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", " |> sprintf "INCLUDE (%s)"
                let predStr =
                    match pars.Predicate with
                    | None -> ""
                    | Some pred -> sprintf "WHERE %O" pred
                String.concatWithWhitespaces ["CREATE"; uniqueStr; suffixStr; includeStr; predStr]
            | SORenameIndex (index, toName) -> sprintf "ALTER INDEX %s RENAME TO %s" (index.ToSQLString()) (toName.ToSQLString())
            | SODropIndex index -> sprintf "DROP INDEX %s" (index.ToSQLString())
            | SOCreateOrReplaceFunction (func, args, def) ->
                assert (Array.length args = Array.length def.Arguments)
                let argsStr = Seq.map2 functionArgumentToString args def.Arguments |> String.concat ", "
                sprintf "CREATE OR REPLACE FUNCTION %s (%s) RETURNS %s LANGUAGE %s %s AS %s"
                    (func.ToSQLString())
                    argsStr
                    (def.ReturnValue.ToSQLString())
                    (def.Language.ToSQLString())
                    (def.Behaviour.ToSQLString())
                    (renderSqlString def.Definition)
            | SORenameFunction (func, args, toName) ->
                let argsStr = args |> Seq.map functionSignatureToString |> String.concat ", "
                sprintf "ALTER FUNCTION %s (%s) RENAME TO %s" (func.ToSQLString()) argsStr (toName.ToSQLString())
            | SODropFunction (func, args) ->
                let argsStr = args |> Seq.map functionSignatureToString |> String.concat ", "
                sprintf "DROP FUNCTION %s (%s)" (func.ToSQLString()) argsStr
            | SOCreateTrigger (trigger, table, def) ->
                assert (not <| Array.isEmpty def.Events)
                let constraintStr = if Option.isSome def.IsConstraint then "CONSTRAINT" else ""
                let eventsStr = def.Events |> Seq.map (fun e -> e.ToSQLString()) |> String.concat " OR "
                let triggerStr =
                    sprintf "TRIGGER %s %s %s ON %s"
                        (trigger.Name.ToSQLString())
                        (def.Order.ToSQLString())
                        eventsStr
                        ({ Schema = trigger.Schema; Name = table }.ToSQLString())
                let deferStr = optionToSQLString def.IsConstraint
                let modeStr = def.Mode.ToSQLString()
                let whenStr =
                    match def.Condition with
                    | None -> ""
                    | Some cond -> sprintf "WHEN (%s)" (cond.Value.ToSQLString())
                let argsStr = def.FunctionArgs |> Seq.map renderSqlString |> String.concat ", "
                let tailStr = sprintf "EXECUTE FUNCTION %s (%s)" (def.FunctionName.ToSQLString()) argsStr
                String.concatWithWhitespaces ["CREATE"; constraintStr; triggerStr; deferStr; modeStr; whenStr; tailStr]
            | SORenameTrigger (trigger, table, toName) ->
                sprintf "ALTER TRIGGER %s ON %s RENAME TO %s" (trigger.Name.ToSQLString()) ({ Schema = trigger.Schema; Name = table }.ToSQLString()) (toName.ToSQLString())
            | SODropTrigger (trigger, table) ->
                sprintf "DROP TRIGGER %s ON %s" (trigger.Name.ToSQLString()) ({ Schema = trigger.Schema; Name = table }.ToSQLString())

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()