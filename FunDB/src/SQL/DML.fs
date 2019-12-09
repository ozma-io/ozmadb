module FunWithFlags.FunDB.SQL.DML

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST

[<NoEquality; NoComparison>]
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

[<NoEquality; NoComparison>]
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

[<NoEquality; NoComparison>]
type InsertExpr =
    { name : TableRef
      columns : (obj * ColumnName)[] // obj is extra metadata
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

[<NoEquality; NoComparison>]
type UpdateExpr =
    { name : TableRef
      columns : Map<ColumnName, obj * ValueExpr> // obj is extra metadata
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

[<NoEquality; NoComparison>]
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
