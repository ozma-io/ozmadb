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
open FunWithFlags.FunDB.SQL.Evaluate
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.FunQL.Meta
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.PrettyPrinter

type EntityId = int

type ViewCell =
    internal { value : QualifiedValue;
               pun : string option;
             } with
        member this.Value = this.value

        member this.IsPunned = Option.isSome this.pun

        // XXX: RenderCellValue and ParseCellValue should be kept in sync.
        (* Note on cultural matters:

        * All data is stored in invariant culture in database.
        * All data is shown to user in their local cultural format when possible.
        * All data inputs are attempted to be parsed from local cultural format when possible.
        * Time is stored as UTC except when we cannot convert it back and forth, but is always entered as local time.
          * For example time in FunQL expressions is local by default.
        *)
        static member RenderCellValue (value : QualifiedValue) : string =
            match value with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> f.ToString ()
                | VString(s) -> s
                | VBool(b) -> b.ToString ()
                | VDateTime(dt) -> dt.ToString ()
                | VDate(d) -> d.ToString ()
                | VObject(obj) -> obj.ToString ()
                | VNull -> ""

        static member internal ParseCellValue (valType : FieldType<_>) (str : string) : FieldValue option =
            if str = ""
            then Some(FNull)
            else
                match valType with
                    | FTInt -> tryIntCurrent str |> Option.map FInt
                    | FTString -> Some(FString(str))
                    | FTBool ->
                        match str.ToLower() with
                            | "true" -> Some(FBool(true))
                            | "false" -> Some(FBool(false))
                            | _ -> None
                    | FTDateTime -> tryDateTimeCurrent str |> Option.map FDateTime
                    | FTDate -> tryDateTimeCurrent str |> Option.map FDate
                    // FIXME: PostgreSQL will verify this reference. However we need to check if a user has access to the referenced field.
                    | FTReference(_) -> tryIntCurrent str |> Option.map FInt
                    | FTEnum(possible) ->
                        if Set.contains str possible then
                            Some(FString(str))
                        else
                            None

        override this.ToString () =
            match this.pun with
                | Some(s) -> s
                | None -> ViewCell.RenderCellValue this.value

type ViewRow =
    internal { cells : ViewCell array;
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
        member this.ValueType = this.valueType

type ViewResult =
    internal { attributes : AttributeMap;
               columns : ViewColumn array;
               rows : ViewRow seq;
             } with
        member this.Attributes = this.attributes
        member this.Columns = this.columns
        member this.Rows = this.rows

type Summaries = (int * ViewCell) array

type TemplateColumn =
    internal { field : Name.QualifiedColumnField;
               attributes : AttributeMap;
               defaultValue : QualifiedValue;
               summaries : Summaries option;
             } with
        member this.Field = this.field
        member this.Default = this.defaultValue
        member this.Attributes = this.attributes
        member this.Summaries =
            match this.summaries with
                | Some(sums) -> sums
                | None -> null

        member this.ToViewColumn () =
            { name = this.field.Field.Name;
              field = Some(QColumnField(this.field));
              attributes = this.attributes;
              valueType = compileFieldType this.field.fieldType;
            }

        member this.ToDefaultCell () =
            { value = this.defaultValue;
              pun = None;
            }

type TemplateResult =
    internal { entity : Name.QualifiedEntity;
               columns : TemplateColumn array;
               attributes : AttributeMap;
             } with
        member this.Entity = this.entity.Entity
        member this.Columns = this.columns
        member this.Attributes = this.attributes

exception UserViewError of string

type ViewResolver internal (dbQuery : QueryConnection, db : DatabaseContext, qualifier : Qualifier) =
    let qualifyQuery parsedQuery =
        try
            qualifier.QualifyQuery parsedQuery
        with
            | QualifierError(msg) ->
                raise <| UserViewError msg

    let toViewColumn (entities : Name.QEntities) (queryAttrs : AttributeMap) (valueType : ValueType) (attrs, res) : ViewColumn =
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

    let toViewColumns (query : Name.QualifiedQuery) (columns : (string * ValueType) array) : ViewColumn array =
        let mutable i = 0
        let processColumn result =
            let (name, _) = columns.[i]
            if name.StartsWith("_Pun_") then
                i <- i + 1
            let (_, typ) = columns.[i]
            i <- i + 1
            toViewColumn query.entities query.expression.attributes typ result
        Array.map processColumn query.expression.results

    let toViewRow (columns : (string * ValueType) array) (row : QualifiedValue array) : ViewRow =
        let newCells = new List<ViewCell>()
        let mutable i = 0
        while i < Array.length row do
            let (name, _) = columns.[i]
            if name.StartsWith("_Pun_") then
                newCells.Add { value = row.[i + 1];
                                pun = Some(ViewCell.RenderCellValue row.[i]);
                             }
                i <- i + 2
            else
                newCells.Add { value = row.[i];
                               pun = None;
                             }
                i <- i + 1
        { cells = newCells.ToArray ();
          attributes = new AttributeMap ();
        }

    let realizeField (field : Name.QualifiedColumnField) (value : string) : ColumnName * LocalValueExpr =
        let value_ =
            if field.Field.Nullable && value = null
            then FNull
            else
                match ViewCell.ParseCellValue field.fieldType value with
                    | Some(r) -> r
                    | None -> raise <| UserViewError(sprintf "Invalid value of field %s" field.Field.Name)
        (field.Field.Name, compileFieldValue value_)

    let punSummary (rawQuery : Name.QualifiedQuery) =
        let newResults = new List<AttributeMap * Name.QualifiedResult>()
        let mutable newEntities = rawQuery.entities
        let mutable newFrom = rawQuery.expression.from

        for (attrs, result) in rawQuery.expression.results do
            let toResolve =
                if attrs.ContainsKey("ResolveSummary")
                then attrs.GetBoolWithDefault(false, [|"ResolveSummary"|])
                else rawQuery.expression.attributes.GetBoolWithDefault(false, [|"ResolveSummary"|])
            if toResolve then
                match result with
                    | RField(Name.QFField(WrappedField(:? ColumnField) as rawField) as column) ->
                        let field = newEntities.[rawField.Entity.Name].columnFields.[rawField.Field.Name]
                        match field.fieldType with
                            | FTReference(rawEntity) when rawEntity.Entity.SummaryFieldId.HasValue ->
                                let entity =
                                    match Map.tryFind rawEntity.Name newEntities with
                                        | Some(e) -> e
                                        | None ->
                                            let entity = qualifier.QualifyEntity rawEntity.Entity
                                            newEntities <- Map.add rawEntity.Name entity newEntities
                                            entity

                                let entityName = Name.QEEntity(rawEntity)
                                newFrom <-
                                    if fromExprContains entityName newFrom
                                    then newFrom
                                    else FJoin(Left, newFrom, FEntity(entityName), FEEq(FEColumn(column), FEColumn(Name.QFEntityId(rawEntity))))

                                db.Entry(entity.Entity).Reference("SummaryField").Load()
                                let newResult = RExpr("_Pun_" + rawField.Field.Name, FEColumn(Name.QFField(WrappedField(entity.Entity.SummaryField))))
                                newResults.Add (attrs, newResult)
                                ()
                            | _ -> ()
                    | _ -> ()

            newResults.Add (attrs, result)

        { rawQuery with
              entities = newEntities;
              expression = { rawQuery.expression with
                                 results = newResults.ToArray ();
                                 from = newFrom;
                           };
        }

    let runQuery (rawQuery : Name.QualifiedQuery) =
        let query = punSummary rawQuery

        let (types, results) = Compiler.compileQuery query |> dbQuery.Query

        { attributes = query.expression.attributes;
          columns = toViewColumns rawQuery types;
          rows = results |> Array.toSeq |> Seq.map (toViewRow types);
        }

    let mainEntity (query : Name.QualifiedQuery) : Name.QualifiedEntity * (Name.QualifiedColumnField array) =
        let entity =
            match query.expression.from with
                | FEntity(Name.QEEntity(e)) -> query.entities.[e.Name]
                | _ -> invalidOp "Cannot get main entity from a complex query"

        let matchField = function
            | (_, RField(Name.QFField(WrappedField(:? ColumnField as f)))) -> Some(entity.columnFields.[f.Name])
            | _ -> None

        (entity, query.expression.results |> seqMapMaybe matchField |> Array.ofSeq)

    // XXX: make this atomic!
    let migrate () =
        let toMeta = buildFunMeta db qualifier
        let fromMeta = getDatabaseMeta dbQuery
        let plan = migrateDatabase fromMeta toMeta
        eprintfn "From meta: %s" (fromMeta.ToString ())
        eprintfn "To meta: %s" (toMeta.ToString ())
        Seq.iter dbQuery.ApplyOperation plan

    let postUpdate (entity : Name.QualifiedEntity) =
        if not entity.Entity.SchemaId.HasValue then
            migrate ()

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
              results = [| (emptyAttrs, RField(idField)); (emptyAttrs, RField(summaryField)) |];
              from = FEntity(Name.QEEntity(qEntity.entity));
              where = None;
              orderBy = [| |];
            }
        let query =
            { Name.QualifiedQuery.expression = queryExpr;
              Name.entities = mapSingleton qEntity.entity.Name qEntity;
            }
        let res = runQuery query
        let toSummary row =
            let id =
                match row.cells.[0].value with
                    | VInt(i) -> i
                    | _ -> failwith "Id is expected to be an integer"
            (id, row.cells.[1])
        res.rows |> Seq.map toSummary |> Array.ofSeq

    member this.GetTemplate (parsedQuery : ParsedQueryExpr) : TemplateResult =
        let query = qualifyQuery parsedQuery
        let (entity, fields) = mainEntity query

        let templateColumn (field : Name.QualifiedColumnField) (attrs, _) : TemplateColumn =
            let summaries =
                match field.fieldType with
                    | FTReference(WrappedEntity(re)) -> Some(this.SelectSummaries re)
                    | _ -> None
            let defaultValue =
                if field.Field.Default = null then
                    if field.Field.Nullable then
                        VNull
                    else
                        match field.fieldType with
                            | FTInt -> VInt(0)
                            | FTString -> VString("")
                            | FTBool -> VBool(false)
                            | FTDateTime -> VDateTime(DateTime.Now)
                            | FTDate -> VDate(DateTime.Today)
                            | FTReference(_) ->
                                let (id, _) = (Option.get summaries).[0]
                                VInt(id)
                            | FTEnum(vals) -> VString(vals |> Set.toSeq |> Seq.head)
                else
                    let lexbuf = LexBuffer<char>.FromString field.Field.Default
                    let expr = Parser.fieldExpr Lexer.tokenstream lexbuf
                    match expr |> Qualifier.QualifyDefaultExpr |> compileFieldExpr |> getPureValueExpr with
                        | Some(e) ->
                            // FIXME: cache results, as they are supposed to be pure (LRU cache would be good).
                            match dbQuery.Evaluate { values = [| ("default", e) |] } with
                                | [| res |] -> res.value
                                | _ -> failwith "Default evaluation is expected to return only one item"
                        | None -> raise <| new NotImplementedException("Non-pure default expressions in fields are not supported for insert queries")
            { field = field;
              defaultValue = defaultValue;
              attributes = attrs;
              summaries = summaries;
            }

        { entity = entity;
          columns = Seq.map2 templateColumn fields query.expression.results |> Array.ofSeq;
          attributes = query.expression.attributes;
        }

    member this.InsertEntry (parsedQuery : ParsedQueryExpr, row : IDictionary<string, string>) =
        let query = qualifyQuery parsedQuery
        let (entity, fields) = mainEntity query
        let (columns, values) = fields |> Seq.map (fun f -> realizeField f row.[f.Field.Name]) |> List.ofSeq |> List.unzip

        dbQuery.Insert { name = makeEntity entity.Entity;
                         columns = Array.ofList columns;
                         values = [| Array.ofList values |];
                       }
        postUpdate entity

    member this.UpdateEntry (parsedQuery : ParsedQueryExpr, id : int, row : IDictionary<string, string>) =
        // TODO: Allow complex multi-entity updates.
        // FIXME: Check that an item with this id is visible for this query expression and lock it for update.
        // Or possibly better, just copy WHERE from parsed expression to the update expression.
        let query = qualifyQuery parsedQuery
        let (entity, fields) = mainEntity query
        let mapValue (f : Name.QualifiedColumnField) =
            match row.TryGetValue f.Field.Name with
                | (true, value) -> Some(realizeField f value)
                | _ -> None
        let values = fields |> seqMapMaybe mapValue |> Array.ofSeq

        dbQuery.Update { name = makeEntity entity.Entity;
                         columns = values;
                         where = Some(VEEq(VEColumn(LocalColumn("Id")), VEValue(VInt(id))));
                       }
        postUpdate entity

    member this.DeleteEntry (parsedQuery : ParsedQueryExpr, id : int) =
        let query = qualifyQuery parsedQuery
        let (entity, fields) = mainEntity query

        dbQuery.Delete { name = makeEntity entity.Entity;
                         where = Some(VEEq(VEColumn(LocalColumn("Id")), VEValue(VInt(id))));
                       }
        postUpdate entity

    member this.Migrate () = migrate ()
