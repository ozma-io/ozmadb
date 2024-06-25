module OzmaDB.SQL.Misc

open OzmaDB.OzmaUtils
open OzmaDB.SQL.Utils
open OzmaDB.SQL.AST

type ExplainFormat =
    | EFText
    | EFJson
    | EFXML
    | EFYaml

    override this.ToString() = this.ToSQLString()

    member this.ToSQLString() =
        match this with
        | EFText -> "TEXT"
        | EFJson -> "JSON"
        | EFXML -> "XML"
        | EFYaml -> "YAML"

    interface ISQLString with
        member this.ToSQLString() = this.ToSQLString()

[<NoEquality; NoComparison>]
type ExplainExpr<'a> when 'a :> ISQLString =
    { Statement: 'a
      Analyze: bool option
      Verbose: bool option
      Costs: bool option
      Format: ExplainFormat option }

    override this.ToString() = this.ToSQLString()

    member this.ToSQLString() =
        let renderOptionBool name x = sprintf "%s %s" name (renderSqlBool x)

        let allOptions =
            seq {
                Option.map (renderOptionBool "ANALYZE") this.Analyze
                Option.map (renderOptionBool "VERBOSE") this.Verbose
                Option.map (renderOptionBool "COSTS") this.Costs
                Option.map (fun x -> sprintf "FORMAT %O" x) this.Format
            }

        let options = Seq.catMaybes allOptions |> Seq.toArray

        let optionsStr =
            if Array.isEmpty options then
                ""
            else
                options |> String.concat ", " |> sprintf "(%s)"

        String.concatWithWhitespaces [ "EXPLAIN"; optionsStr; this.Statement.ToSQLString() ]

    interface ISQLString with
        member this.ToSQLString() = this.ToSQLString()

type SetScope =
    | SSSession
    | SSLocal

    override this.ToString() = this.ToSQLString()

    member this.ToSQLString() =
        match this with
        | SSSession -> "SESSION"
        | SSLocal -> "LOCAL"

    interface ISQLString with
        member this.ToSQLString() = this.ToSQLString()

type SetValue =
    | SVDefault
    | SVValue of Value[]

    override this.ToString() = this.ToSQLString()

    member this.ToSQLString() =
        match this with
        | SVDefault -> "DEFAULT"
        | SVValue vals ->
            assert (not <| Array.isEmpty vals)
            vals |> Seq.map (fun x -> x.ToSQLString()) |> String.concat ", "

    interface ISQLString with
        member this.ToSQLString() = this.ToSQLString()

type SetExpr =
    { Parameter: ParameterName
      Scope: SetScope option
      Value: SetValue }

    override this.ToString() = this.ToSQLString()

    member this.ToSQLString() =
        let scopeStr =
            this.Scope |> Option.map (fun x -> x.ToSQLString()) |> Option.defaultValue ""

        String.concatWithWhitespaces [ "SET"; scopeStr; this.Parameter.ToSQLString(); "="; this.Value.ToSQLString() ]

    interface ISQLString with
        member this.ToSQLString() = this.ToSQLString()
