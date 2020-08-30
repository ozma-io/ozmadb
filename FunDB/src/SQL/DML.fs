module FunWithFlags.FunDB.SQL.DML

open FunWithFlags.FunUtils
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
    { Name : TableRef
      Columns : (obj * ColumnName)[] // obj is extra metadata
      Values : InsertValues
      Returning : SelectedColumn[]
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let returningStr =
                if Array.isEmpty this.Returning then
                    ""
                else
                    let resultsStr = this.Returning |> Seq.map (fun res -> res.ToSQLString()) |> String.concat ", "
                    sprintf "RETURNING %s" resultsStr
            let insertStr =
                sprintf "INSERT INTO %s (%s) %s"
                    (this.Name.ToSQLString())
                    (this.Columns |> Seq.map (fun (extra, x) -> x.ToSQLString()) |> String.concat ", ")
                    (this.Values.ToSQLString())
            String.concatWithWhitespaces [insertStr; returningStr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type UpdateExpr =
    { Name : TableRef
      Columns : Map<ColumnName, obj * ValueExpr> // obj is extra metadata
      Where : ValueExpr option
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            assert (not <| Map.isEmpty this.Columns)

            let valuesExpr = this.Columns |> Map.toSeq |> Seq.map (fun (name, (extra, expr)) -> sprintf "%s = %s" (name.ToSQLString()) (expr.ToSQLString())) |> String.concat ", "
            let condExpr =
                match this.Where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            let updateStr = sprintf "UPDATE %s SET %s" (this.Name.ToSQLString()) valuesExpr
            String.concatWithWhitespaces [updateStr; condExpr]

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()

[<NoEquality; NoComparison>]
type DeleteExpr =
    { Name : TableRef
      Where : ValueExpr option
      Extra : obj
    } with
        override this.ToString () = this.ToSQLString()

        member this.ToSQLString () =
            let condExpr =
                match this.Where with
                | Some c -> sprintf "WHERE %s" (c.ToSQLString())
                | None -> ""
            sprintf "DELETE FROM %s" (String.concatWithWhitespaces [this.Name.ToSQLString(); condExpr])

        interface ISQLString with
            member this.ToSQLString () = this.ToSQLString()
