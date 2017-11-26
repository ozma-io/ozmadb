namespace FunWithFlags.FunDB.View

open System
open System.Runtime.InteropServices
open System.Linq
open System.Collections.Generic
open Microsoft.FSharp.Text.Lexing
open YC.PrettyPrinter.Pretty

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.FunQL
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Meta
open FunWithFlags.FunDB.SQL.Migration
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.FunQL.Meta
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.PrettyPrinter

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
               valueType : ValueType option;
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
               rows : ViewRow seq;
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

    let toViewColumn valueType (res, attrs) =
        { name = Name.resultName res;
          field = match res with
                      | RField(Name.QFField(f)) -> Some(f)
                      | _ -> None
          attributes = attrs;
          valueType = valueType;
        }

    let valueToDisplayString = function
        | VInt(i) -> i.ToString ()
        | VFloat(f) -> f.ToString ()
        | VString(s) -> s
        | VBool(b) -> b.ToString ()
        | VDateTime(dt) -> dt.ToString ()
        | VDate(d) -> d.ToString ()
        | VObject(obj) -> obj.ToString ()
        | VNull -> null

    let simpleFieldValue (valType : FieldType<_>) (str : string) : FieldValue option =
        if str = null
        then Some(FNull)
        else
        try
            match valType with
                | FTInt -> Some(FInt(int str))
                | FTString -> Some(FString(str))
                | FTBool ->
                    match str.ToLower() with
                        | "true" -> Some(FBool(true))
                        | "false" -> Some(FBool(false))
                        | _ -> None
                | FTDateTime -> Some(FDateTime(DateTime.Parse(str)))
                | FTDate -> Some(FDate(DateTime.Parse(str)))
                // XXX: Maybe some kind of check here.
                | FTReference(_) -> Some(FInt(int str))
        with
            _ -> None

    let toViewRow row =
        { cells = Array.map valueToDisplayString row;
          attributes = new AttributeMap()
        }

    let realizeFields (entity : Entity) (row : IDictionary<string, string>) : (ColumnName * LocalValueExpr) seq =
        db.Entry(entity).Collection("Fields").Load()

        let toValueType name =
            let lexbuf = LexBuffer<char>.FromString name
            Parser.fieldType Lexer.tokenstream lexbuf
            
        let types = entity.Fields |> Seq.map (fun f -> (f.Name, (f, toValueType f.Type))) |> Map.ofSeq

        let toValue = function
            | KeyValue(k, v) ->
                let (field, ftype) = types.[k]
                let value =
                    if field.Nullable && v = null
                    then FNull
                    else
                        match simpleFieldValue ftype v with
                            | Some(r) -> r
                            | None -> raise <| UserViewError(sprintf "Invalid value of field %s" k)
                (k, VEValue(compileFieldValue value))

        row |> Seq.map toValue

    static member ParseQuery (uv : UserView) =
        let lexbuf = LexBuffer<char>.FromString uv.Query
        try
            Parser.query Lexer.tokenstream lexbuf
        with
            | Failure(msg) -> raise <| UserViewError msg

    // Make this return an IDisposable cursor.
    member this.RunQuery (parsedQuery : ParsedQueryExpr) =
        let queryTree = qualifyQuery parsedQuery
        let (types, results) = Compiler.compileQuery queryTree |> dbQuery.Query
        let columns = queryTree.results |> Array.map2 toViewColumn types

        { attributes = queryTree.attributes;
          columns = columns;
          rows = results |> Array.toSeq |> Seq.map toViewRow;
        }

    member this.GetTemplate (entity : Entity) =
        db.Entry(entity).Reference("Fields").Load()

        let templateColumn (field : Field) =
            let defaultValue =
                if field.Default = null then
                    null
                else
                    let lexbufDefault = LexBuffer<char>.FromString field.Default
                    let defVal = Parser.fieldExpr Lexer.tokenstream lexbufDefault
                    print 80 <| ppFieldExpr defVal
            { field = field;
              defaultValue = defaultValue;
            }

        entity.Fields.Select(templateColumn).ToArray()

    member this.InsertEntry (entity : Entity, row : IDictionary<string, string>) =
        db.Entry(entity).Reference("Schema").Load()

        let (columns, values) = realizeFields entity row |> List.ofSeq |> List.unzip

        dbQuery.Insert { name = makeEntity entity;
                         columns = Array.ofList columns;
                         values = [| Array.ofList values |];
                       }

        if not entity.SchemaId.HasValue then
                this.Migrate ()

    member this.UpdateEntry (entity : Entity, id : int, row : IDictionary<string, string>) =
        db.Entry(entity).Reference("Schema").Load()

        let values = realizeFields entity row |> Array.ofSeq
        dbQuery.Update { name = makeEntity entity;
                         columns = values;
                         where = Some(VEEq(VEColumn(LocalColumn("Id")), VEValue(VInt(id))));
                       }

        if not entity.SchemaId.HasValue then
                this.Migrate ()

    member this.DeleteEntry (entity : Entity, id : int) =
        db.Entry(entity).Reference("Schema").Load()

        dbQuery.Delete { name = makeEntity entity;
                         where = Some(VEEq(VEColumn(LocalColumn("Id")), VEValue(VInt(id))));
                       }

        if not entity.SchemaId.HasValue then
                this.Migrate ()

    // XXX: make this atomic!
    member this.Migrate () =
        let toMeta = buildFunMeta db qualifier
        let fromMeta = getDatabaseMeta dbQuery
        let plan = migrateDatabase fromMeta toMeta
        printfn "From meta: %s" (fromMeta.ToString ())
        printfn "To meta: %s" (toMeta.ToString ())
        Seq.iter dbQuery.ApplyOperation plan
