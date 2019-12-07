module FunWithFlags.FunDB.SQL.DDL

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST

[<NoComparison>]
type InsertValue =
    | IVValue of ValueExpr
    | IVDefault
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IVValue e -> e.ToSQLString()
            | IVDefault -> "DEFAULT"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type InsertValues =
    | IValues of InsertValue[][]
    | IDefaults
    with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            match this with
            | IValues values ->
                let renderInsertValue (values : InsertValue[]) =
                    values |> Seq.map (fun v -> v.ToSQLString()) |> String.concat ", " |> sprintf "(%s)"

                assert (not <| Array.isEmpty values)
                sprintf "VALUES %s" (values |> Seq.map renderInsertValue |> String.concat ", ")
            | IDefaults -> "DEFAULT VALUES"

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type InsertExpr =
    { name : TableRef
      columns : (obj * ColumnName)[] // with extra metadata
      values : InsertValues
      returning : SelectedColumn[]
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let returningStr =
                if Array.isEmpty this.returning then
                    ""
                else
                    let resultsStr = this.returning |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let insertStr =
                sprintf "INSERT INTO %s (%s) %s"
                    (this.name.ToSQLString())
                    (this.columns |> Seq.map (fun (extra, x) -> x.ToSQLString()) |> String.concat ", ")
                    (this.values.ToSQLString())
            concatWithWhitespaces [insertStr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type UpdateExpr =
    { name : TableRef
      columns : Map<ColumnName, obj * ValueExpr> // with extra metadata
      where : ValueExpr option
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not <| Map.isEmpty this.columns)

            let valuesExpr = this.columns |> Map.toSeq |> Seq.map (fun (name, (extra, expr)) -> sprintf "%s = %s" (name.ToSQLString()) (expr.ToSQLString())) |> String.concat ", "
            let condExpr =
                match this.where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            let updateStr = sprintf "UPDATE %s SET %s" (this.name.ToSQLString()) valuesExpr
            concatWithWhitespaces [updateStr; condExpr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoComparison>]
type DeleteExpr =
    { name : TableRef
      where : ValueExpr option
      extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let condExpr =
                match this.where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            sprintf "DELETE FROM %s" (concatWithWhitespaces [this.name.ToSQLString(); condExpr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

// Meta

[<NoComparison>]
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

[<NoComparison>]
type ConstraintMeta =
    | CMUnique of ColumnName[]
    | CMCheck of ValueExpr
    | CMPrimaryKey of ColumnName[]
    | CMForeignKey of TableRef * (ColumnName * ColumnName)[]

[<NoComparison>]
type IndexMeta =
    { columns : ColumnName[]
    }

[<NoComparison>]
type TableMeta =
    { columns : Map<ColumnName, ColumnMeta>
    }

let emptyTableMeta =
    { columns = Map.empty
    }

let unionTableMeta (a : TableMeta) (b : TableMeta) =
    { columns = Map.unionUnique a.columns b.columns
    }

[<NoComparison>]
type ObjectMeta =
    | OMTable of TableMeta
    | OMSequence
    | OMConstraint of TableName * ConstraintMeta
    | OMIndex of TableName * IndexMeta

[<NoComparison>]
type SchemaMeta =
    { objects : Map<SQLName, ObjectMeta>
    }

let emptySchemaMeta =
    { objects = Map.empty
    }

[<NoComparison>]
type DatabaseMeta =
    { schemas : Map<SchemaName, SchemaMeta>
    }

[<NoComparison>]
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

[<NoComparison>]
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

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()