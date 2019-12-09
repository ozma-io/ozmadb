module FunWithFlags.FunDB.SQL.PLPgSQL

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.DDL

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
    { format : string
      options : ValueExpr[]
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let args = this.options |> Seq.map (fun o -> sprintf ", %s" (o.ToSQLString())) |> String.concat ""
            concatWithWhitespaces [renderSqlString this.format; args]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

[<NoEquality; NoComparison>]
type Statement =
    | StDefinition of SchemaOperation
    | StIfThenElse of (ValueExpr * Statements)[] * Statements option
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
                    concatWithWhitespaces [firstStr; otherCases; elseStr; "END IF"]
                | StRaise raise -> raise.ToPLPgSQLString()
                | StReturn expr -> sprintf "RETURN %s" (expr.ToSQLString())
            sprintf "%s;" str

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

and [<NoEquality; NoComparison>] RaiseStatement =
    { level : RaiseLevel
      message : RaiseMessage option
      options : Map<RaiseOption, ValueExpr>
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            let initStr = sprintf "RAISE %s" (this.level.ToPLPgSQLString())
            let messageStr =
                match this.message with
                | None -> ""
                | Some m -> m.ToPLPgSQLString()
            let optionsStr =
                if Map.isEmpty this.options then
                    ""
                else
                    let opts = this.options |> Map.toSeq |> Seq.map (fun (opt, v) -> sprintf "%s = %s" (opt.ToPLPgSQLString()) (v.ToSQLString())) |> String.concat ", "
                    sprintf "USING %s" opts
            concatWithWhitespaces [initStr; messageStr; optionsStr]

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

and Statements = Statement[]

[<NoEquality; NoComparison>]
type Program =
    { body : Statements
    } with
        override this.ToString () = this.ToPLPgSQLString()

        member this.ToPLPgSQLString () =
            sprintf "BEGIN %s END" (Statement.StatementsToString this.body)

        interface IPLPgSQLString with
            member this.ToPLPgSQLString () = this.ToPLPgSQLString()

let plPgSQLName = SQLName "plpgsql"