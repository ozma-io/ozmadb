namespace FunWithFlags.FunDB.View

open System
open System.Runtime.InteropServices
open System.Linq
open System.Collections.Generic
open Microsoft.FSharp.Text.Lexing
open YC.PrettyPrinter.Pretty

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Utils
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

type QualifiedField =
    | QColumnField of Name.QualifiedColumnField
    | QComputedField of Name.QualifiedComputedField

    member this.Field =
        match this with
            | QColumnField(f) -> f.Field :> Field
            | QComputedField(f) -> f.Field :> Field

    member this.TryColumnField () =
        match this with
            | QColumnField(f) -> f
            | _ -> Unchecked.defaultof<Name.QualifiedColumnField>

    member this.TryComputedField () =
        match this with
            | QComputedField(f) -> f
            | _ -> Unchecked.defaultof<Name.QualifiedComputedField>

type ViewColumn =
    internal { name : string;
               field : QualifiedField option;
               attributes : AttributeMap;
               valueType : ValueType;
             } with
        member this.Name = this.name
        member this.Field =
            match this.field with
                | Some(f) -> f
                | None -> Unchecked.defaultof<QualifiedField>
        member this.Attributes = this.attributes

type ViewResult =
    internal { attributes : AttributeMap;
               columns : ViewColumn array;
               rawColumns : ViewColumn array;
               rows : ViewRow seq;
             } with
        member this.Attributes = this.attributes
        member this.Columns = this.columns
        member this.RawColumns = this.rawColumns
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

    let toViewColumn (entities : Name.QEntities) (queryAttrs : AttributeMap) (valueType : ValueType) (res, attrs) =
        { name = Name.resultName res;
          field =
              match res with
                  | RField(Name.QFField(f)) ->
                      let entity = entities.[f.Entity.Name]
                      let field =
                          match Map.tryFind f.Field.Name entity.columnFields with
                              | Some(cf) -> QColumnField(cf)
                              | None -> QComputedField(entity.computedFields.[f.Field.Name])
                      Some(field)
                  | _ -> None
          attributes = AttributeMap.Merge queryAttrs attrs;
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

    let toViewRow (column : ViewColumn) row =
        { cells = Array.map valueToDisplayString row;
          attributes = column.attributes;
        }

    let realizeFields (entity : Entity) (row : IDictionary<string, string>) : (ColumnName * LocalValueExpr) seq =
        let qEntity = qualifier.QualifyEntity entity

        let toValue = function
            | KeyValue(k, v) ->
                let field = qEntity.columnFields.[k]
                let value =
                    if field.Field.Nullable && v = null
                    then FNull
                    else
                        match simpleFieldValue field.fieldType v with
                            | Some(r) -> r
                            | None -> raise <| UserViewError(sprintf "Invalid value of field %s" k)
                (k, VEValue(compileFieldValue value))

        row |> Seq.map toValue

    let replaceSummary (globalAttrs : AttributeMap) (oldResults, oldEntities : Name.QEntities, oldFrom) (result : Name.QualifiedResult, attrs : AttributeMap) =
        let toResolve =
            if attrs.ContainsKey("ResolveSummary")
            then attrs.GetBoolWithDefault(false, [|"ResolveSummary"|])
            else globalAttrs.GetBoolWithDefault(false, [|"ResolveSummary"|])
        if toResolve
        then
            let (newResult, newEntities, newFrom) =
                match result with
                    | RField(Name.QFField(WrappedField(:? ColumnField) as rawField) as column) ->
                        let field = oldEntities.[rawField.Entity.Name].columnFields.[rawField.Field.Name]
                        match field.fieldType with
                            | FTReference(rawEntity) when rawEntity.Entity.SummaryFieldId.HasValue ->
                                let (newEntities, entity) =
                                    match Map.tryFind rawEntity.Name oldEntities with
                                        | Some(e) -> (oldEntities, e)
                                        | None ->
                                            let entity = qualifier.QualifyEntity rawEntity.Entity
                                            (Map.add rawEntity.Name entity oldEntities, entity)
                                let entityName = Name.QEEntity(rawEntity)
                                db.Entry(entity.Entity).Reference("SummaryField").Load()
                                let newResult = RField(Name.QFField(WrappedField(entity.Entity.SummaryField)))
                                let newFrom =
                                    if fromExprContains entityName oldFrom
                                    then oldFrom
                                    else FJoin(Left, oldFrom, FEntity(entityName), FEEq(FEColumn(column), FEColumn(Name.QFEntityId(rawEntity))))
                                (newResult, newEntities, newFrom)
                            | _ -> (result, oldEntities, oldFrom)
                    | _ -> (result, oldEntities, oldFrom)
            ((newResult, attrs) :: oldResults, newEntities, newFrom)
        else
            ((result, attrs) :: oldResults, oldEntities, oldFrom)

    let runQuery (rawQuery : Name.QualifiedQuery) =
        let (newResults, newEntities, newFrom) = Array.fold (replaceSummary rawQuery.expression.attributes) ([], rawQuery.entities, rawQuery.expression.from) rawQuery.expression.results
        let query = { rawQuery with
                          entities = newEntities;
                          expression = { rawQuery.expression with
                                             results = newResults |> List.toArray |> Array.rev;
                                             from = newFrom;
                                       };
                    }

        let (types, results) = Compiler.compileQuery query |> dbQuery.Query
        let columns = query.expression.results |> Array.map2 (toViewColumn query.entities query.expression.attributes) types
        let rawColumns = rawQuery.expression.results |> Array.map2 (toViewColumn rawQuery.entities rawQuery.expression.attributes) types

        { attributes = query.expression.attributes;
          columns = columns;
          rawColumns = rawColumns;
          rows = results |> Array.toSeq |> Seq.map2 toViewRow columns;
        }

    static member ParseQuery (uv : UserView) =
        let lexbuf = LexBuffer<char>.FromString uv.Query
        try
            Parser.query Lexer.tokenstream lexbuf
        with
            | Failure(msg) -> raise <| UserViewError msg

    // Make this return an IDisposable cursor.
    member this.RunQuery (parsedQuery : ParsedQueryExpr) =
        let rawQuery = qualifyQuery parsedQuery
        runQuery rawQuery

    member this.SelectSummaries (entity : Entity) =
        let qEntity = qualifier.QualifyEntity entity
        db.Entry(entity).Reference("SummaryField").Load()
        let idField = Name.QFEntityId(qEntity.entity)
        let summaryField =
            if entity.SummaryFieldId.HasValue
            then Name.QFField(WrappedField(entity.SummaryField))
            else idField
        let emptyAttrs = new AttributeMap ()
        let queryExpr =
            { attributes = emptyAttrs;
              results = [| (RField(idField), emptyAttrs); (RField(summaryField), emptyAttrs) |];
              from = FEntity(Name.QEEntity(qEntity.entity));
              where = None;
              orderBy = [| |];
            }
        let query =
            { Name.QualifiedQuery.expression = queryExpr;
              Name.entities = mapSingleton qEntity.entity.Name qEntity;
            }
        runQuery query

    member this.GetTemplate (entity : Entity) =
        db.Entry(entity).Collection("Fields").Load()

        let templateColumn (field : ColumnField) =
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

        entity.ColumnFields.Select(templateColumn).ToArray()

    member this.InsertEntry (entity : Entity, row : IDictionary<string, string>) =
        let (columns, values) = realizeFields entity row |> List.ofSeq |> List.unzip

        dbQuery.Insert { name = makeEntity entity;
                         columns = Array.ofList columns;
                         values = [| Array.ofList values |];
                       }

        if not entity.SchemaId.HasValue then
                this.Migrate ()

    member this.UpdateEntry (entity : Entity, id : int, row : IDictionary<string, string>) =
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
        eprintfn "From meta: %s" (fromMeta.ToString ())
        eprintfn "To meta: %s" (toMeta.ToString ())
        Seq.iter dbQuery.ApplyOperation plan
