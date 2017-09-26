namespace FunWithFlags.FunDB.View

open System.Runtime.InteropServices
open System.Linq
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.Query
open FunWithFlags.FunDB.FunQL
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier

type ViewRow =
    internal { cells : string array;
               attributes : AttributeMap;
             } with
        member this.Cells = this.cells
        member this.Attributes = this.attributes

type ViewColumn =
    internal { name : string;
               field : Field option;
               attributes : AttributeMap;
             } with
        member this.Name = this.name
        member this.Field =
            match this.field with
                | Some(f) -> f
                | None -> null
        member this.Attributes = this.attributes

type ViewResult =
    internal { columns : ViewColumn array;
               rows : ViewRow array;
             } with
        member this.Columns = this.columns
        member this.Rows = this.rows

exception UserViewError of string

type ViewResolver internal (dbQuery : QueryConnection, db : DatabaseContext, qualifier : Qualifier) =
    let qualifyQuery parsedQuery =
        try
            qualifier.Qualify parsedQuery
        with
            | QualifierError(msg) ->
                printf "Qualifier error: %s" msg
                raise <| UserViewError msg

    let toViewColumn (res, attrs) =
        { name = Name.resultName res;
          field = match res with
                      | RField(Name.QFField(f)) -> Some(f)
                      | _ -> None
          attributes = attrs;
        }

    let toViewRow row =
        { cells = row;
          attributes = new AttributeMap()
        }

    static member ParseQuery (uv : UserView) =
        let lexbuf = LexBuffer<char>.FromString uv.Query
        try
            Parser.start Lexer.tokenstream lexbuf
        with
            | Failure(msg) -> raise <| UserViewError msg

    member this.RunQuery (parsedQuery : ParsedQueryExpr) =
        let queryTree = qualifyQuery parsedQuery
        let columns = queryTree.results |> Array.map toViewColumn
        let results = Compiler.compileQuery queryTree |> dbQuery.Query

        { columns = columns;
          rows = Array.map toViewRow results;
        }
