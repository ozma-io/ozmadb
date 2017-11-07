namespace FunWithFlags.FunDB.View

open System.Runtime.InteropServices
open System.Linq
open System.Collections.Generic
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.FunQL
open FunWithFlags.FunDB.SQL.Parse
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier

type EntityId = int

type ViewRow =
    internal { cells : string array;
               attributes : AttributeMap;
             } with
        member this.Cells = this.cells
        member this.Attributes = this.attributes
        member this.IDs = this.attributes

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
    internal { attributes : AttributeMap;
               columns : ViewColumn array;
               rows : ViewRow array;
             } with
        member this.Attributes = this.attributes
        member this.Columns = this.columns
        member this.Rows = this.rows

type TemplateColumn =
    internal { field : Field;
               defaultValue : string;
             } with
        member this.Field = this.field
        member this.Default = this.defaultValue

exception UserViewError of string

type ViewResolver internal (dbQuery : QueryConnection, db : DatabaseContext, qualifier : Qualifier) =
    let qualifyQuery parsedQuery =
        try
            qualifier.QualifyQuery parsedQuery
        with
            | QualifierError(msg) ->
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

    let realizeFields (entity : Entity) (row : IDictionary<string, string>) =
        db.Entry(entity).Collection("Fields").Load()

        let toValueType name =
            let lexbuf = LexBuffer<char>.FromString name
            let ftype = Parser.fieldType Lexer.tokenstream lexbuf
            Compiler.compileValueType ftype
            
        let types = entity.Fields |> Seq.map (fun f -> (f.Name, (f, toValueType f.Type))) |> Map.ofSeq

        let toValue = function
            | KeyValue(k, v) ->
                let (field, ftype) = types.[k]
                let value =
                    if field.Nullable && v = null
                    then VNull
                    else
                        match parseValue ftype v with
                            | Some(r) -> r
                            | None -> raise <| UserViewError(sprintf "Invalid value of field %s" k)
                (k, WValue(value))

        row |> Seq.map toValue

    static member ParseQuery (uv : UserView) =
        let lexbuf = LexBuffer<char>.FromString uv.Query
        try
            Parser.query Lexer.tokenstream lexbuf
        with
            | Failure(msg) -> raise <| UserViewError msg

    member this.RunQuery (parsedQuery : ParsedQueryExpr) =
        let queryTree = qualifyQuery parsedQuery
        let columns = queryTree.results |> Array.map toViewColumn
        let results = Compiler.compileQuery queryTree |> dbQuery.Query

        { attributes = queryTree.attributes;
          columns = columns;
          rows = Array.map toViewRow results;
        }

    member this.GetTemplate (entity : Entity) =
        db.Entry(entity).Reference("Fields").Load()

        let templateColumn (field : Field) =
            let defaultValue =
                if field.Default = null then
                    null
                else
                    let lexbufDefault = LexBuffer<char>.FromString field.Default
                    let defVal = Parser.value Lexer.tokenstream lexbufDefault
                    defVal.ToString ()
            { field = field;
              defaultValue = defaultValue;
            }

        entity.Fields.Select(templateColumn).ToArray()

    member this.InsertEntry (entity : Entity, row : IDictionary<string, string>) =
        db.Entry(entity).Reference("Schema").Load()

        let (columns, values) = realizeFields entity row |> List.ofSeq |> List.unzip

        dbQuery.Insert { name = Compiler.makeEntity entity;
                         columns = Array.ofList columns;
                         values = [| Array.ofList values |];
                       }

    member this.UpdateEntry (entity : Entity, id : int, row : IDictionary<string, string>) =
        db.Entry(entity).Reference("Schema").Load()

        let values = realizeFields entity row |> Array.ofSeq
        dbQuery.Update { name = Compiler.makeEntity entity;
                         columns = values;
                         where = Some(WEq(WColumn(LocalColumn("Id")), WValue(VInt(id))));
                       }

    member this.DeleteEntry (entity : Entity, id : int) =
        db.Entry(entity).Reference("Schema").Load()

        dbQuery.Delete { name = Compiler.makeEntity entity;
                         where = Some(WEq(WColumn(LocalColumn("Id")), WValue(VInt(id))));
                       }
