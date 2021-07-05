module FunWithFlags.FunDB.SQL.PLPgSQL

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL
open FunWithFlags.FunDB.SQL.DML

type IPLPgSQLString =
    abstract member ToPLPgSQLString : unit -> string

type RaiseLevel =
    | RLDebug
    | RLLog
    | RLInfo
    | RLNotice
    | RLWarning
    | RLException
    with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            match this with
            | RLDebug -> "DEBUG"
            | RLLog -> "LOG"
            | RLInfo -> "INFO"
            | RLNotice -> "NOTICE"
            | RLWarning -> "WARNING"
            | RLException -> "EXCEPTION"

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

type RaiseOption =
    | ROMessage
    | RODetail
    | ROHint
    | ROErrcode
    | ROColumn
    | ROConstraint
    | RODatatype
    | ROTable
    | ROSchema
    with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            match this with
            | ROMessage -> "MESSAGE"
            | RODetail -> "DETAIL"
            | ROHint -> "HINT"
            | ROErrcode -> "ERRCODE"
            | ROColumn -> "COLUMN"
            | ROConstraint -> "CONSTRAINT"
            | RODatatype -> "DATATYPE"
            | ROTable -> "TABLE"
            | ROSchema -> "SCHEMA"

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

[<NoEquality; NoComparison>]
type RaiseMessage =
    { Format : string
      Options : ValueExpr[]
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let args = this.Options |> Seq.map (fun o -> sprintf ", %s" (o.ToSQLString())) |> String.concat ""
            String.concatWithWhitespaces [renderSqlString this.Format; args]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

[<NoEquality; NoComparison>]
type Statement =
    | StDefinition of SchemaOperation
    | StIfThenElse of (ValueExpr * Statements)[] * Statements option
    | StUpdate of UpdateExpr
    | StInsert of InsertExpr
    | StDelete of DeleteExpr
    | StRaise of RaiseStatement
    | StReturn of ValueExpr
    with
        override this.ToString () = this.ToPLPgSQLString()

        static member StatementsToString (stmts : Statement seq) =
            stmts |> Seq.map (fun s -> s.ToPLPgSQLString()) |> String.concat " "

        member this.ToPLPgSQLString () =
            let stmtsToString = Statement.StatementsToString
            let str =
                match this with
                | StDefinition op -> op.ToSQLString()
                | StIfThenElse (ifs, els) ->
                    assert (not <| Array.isEmpty ifs)
                    let (firstCase, firstStmts) = ifs.[0]
                    let firstStr = sprintf "IF %s THEN %s" (firstCase.ToSQLString()) (stmtsToString firstStmts)
                    let otherCases =
                        ifs
                            |> Seq.tail
                            |> Seq.map (fun (case, stmts) -> sprintf "ELSIF %s THEN %s" (case.ToSQLString()) (stmtsToString stmts))
                            |> String.concat " "
                    let elseStr =
                        match els with
                        | None -> ""
                        | Some stmts -> sprintf "ELSE %s" (stmtsToString stmts)
                    String.concatWithWhitespaces [firstStr; otherCases; elseStr; "END IF"]
                | StUpdate update -> update.ToSQLString()
                | StInsert insert -> insert.ToSQLString()
                | StDelete delete -> delete.ToSQLString()
                | StRaise raise -> raise.ToPLPgSQLString()
                | StReturn expr -> sprintf "RETURN %s" (expr.ToSQLString())
            sprintf "%s;" str

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

and [<NoEquality; NoComparison>] RaiseStatement =
    { Level : RaiseLevel
      Message : RaiseMessage option
      Options : Map<RaiseOption, ValueExpr>
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let initStr = sprintf "RAISE %s" (this.Level.ToPLPgSQLString())
            let messageStr =
                match this.Message with
                | None -> ""
                | Some m -> m.ToPLPgSQLString()
            let optionsStr =
                if Map.isEmpty this.Options then
                    ""
                else
                    let opts = this.Options |> Map.toSeq |> Seq.map (fun (opt, v) -> sprintf "%s = %s" (opt.ToPLPgSQLString()) (v.ToSQLString())) |> String.concat ", "
                    sprintf "USING %s" opts
            String.concatWithWhitespaces [initStr; messageStr; optionsStr]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

and Statements = Statement[]

[<NoEquality; NoComparison>]
type Declaration =
    { Name : SQLName
      Type : DBValueType
      IsConstant : bool
      IsNullable : bool
      Default : ValueExpr option
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let nameStr = this.Name.ToSQLString()
            let constantStr = if this.IsConstant then "CONSTANT" else ""
            let typeStr = this.Type.ToSQLString()
            let nullableStr = if this.IsNullable then "NULLABLE" else ""
            let defaultStr =
                match this.Default with
                | None -> ""
                | Some expr -> sprintf ":= %s" (expr.ToSQLString())
            String.concatWithWhitespaces [nameStr; constantStr; typeStr; nullableStr; defaultStr]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

[<NoEquality; NoComparison>]
type Program =
    { Declarations : Declaration[]
      Body : Statements
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let declarations =
                if Array.isEmpty this.Declarations then
                    ""
                else
                    sprintf "DECLARE %s" (this.Declarations |> Seq.map (fun x -> x.ToPLPgSQLString()) |> String.concat "; ")
            let body = sprintf "BEGIN %s END" (Statement.StatementsToString this.Body)
            String.concatWithWhitespaces [declarations; body]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

let plPgSQLName = SQLName "plpgsql"