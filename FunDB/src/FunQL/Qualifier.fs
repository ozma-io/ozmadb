namespace FunWithFlags.FunDB.FunQL.Qualifier

open System
open System.Linq
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser

exception QualifierError of string

module Name =
    let internal renderEntityName (e : Entity) =
        if e.SchemaId.HasValue then
            sprintf "%s.%s" (renderSqlName e.Schema.Name) (renderSqlName e.Name)
        else
            renderSqlName e.Name

    let internal renderFieldName (f : Field) = sprintf "%s.%s" (renderEntityName f.Entity) (renderSqlName f.Name)

    type QFieldName =
        | QFField of WrappedField<Field>
        | QFEntityId of WrappedEntity
        | QFSubquery of TableName * ColumnName
        with
            override this.ToString () =
                match this with
                    | QFField(f) -> f.ToString ()
                    | QFEntityId(e) -> sprintf "%s.\"Id\"" (e.ToString ())
                    | QFSubquery(tableName, columnName) -> sprintf "%s.%s" (renderSqlName tableName) (renderSqlName columnName)

    type QEntityName =
        internal
        | QEEntity of WrappedEntity
        | QESubquery of TableName
        with
            override this.ToString () =
                match this with
                    | QEEntity(e) -> e.ToString ()
                    | QESubquery(name) -> renderSqlName name

    type QualifiedColumnField =
        internal { field : WrappedField<ColumnField>;
                   fieldType : FieldType<WrappedEntity>;
                   } with
            member this.Field = this.field.Field
            member this.FieldType = this.fieldType

            override this.ToString () = this.field.ToString ()

    type QualifiedComputedField =
        internal { field : WrappedField<ComputedField>;
                   expression : FieldExpr<QFieldName>;
                 } with
            member this.Field = this.field.Field
            member this.Expression = this.expression

            override this.ToString () = this.field.ToString ()

    type QualifiedEntity =
        internal { entity : WrappedEntity;
                   columnFields : Map<string, QualifiedColumnField>;
                   computedFields : Map<string, QualifiedComputedField>;
                 } with
                member this.Entity = this.entity.Entity
                member this.ColumnFields = this.columnFields
                member this.ComputedFields = this.computedFields

                override this.ToString () = this.entity.ToString ()

    let internal resultName = function
        | RField(QFField(field)) -> field.Field.Name
        | RField(QFEntityId(entity)) -> "Id"
        | RField(QFSubquery(entityName, fieldName)) -> fieldName
        | RExpr(name, e) -> name

    type QualifiedResult = Result<QFieldName>

    type QualifiedQueryExpr = QueryExpr<QEntityName, QFieldName>

    type QualifiedFromExpr = FromExpr<QEntityName, QFieldName>

    type QualifiedFieldType = FieldType<WrappedEntity>

    type QualifiedFieldExpr = FieldExpr<QFieldName>

    type QEntities = Map<EntityName, QualifiedEntity>

    type QualifiedQuery =
        { expression : QueryExpr<QEntityName, QFieldName>;
          entities : QEntities;
        }

open Name

type private QMappedEntity =
    | QMEntity of QualifiedEntity * Map<ColumnName, WrappedField<Field>>
    | QMSubquery of TableName * Set<ColumnName>

type private QMapping = Map<EntityName, QMappedEntity>

type Qualifier internal (db : DatabaseContext) =
    let rec qualifyComputedField (columns : Map<string, QualifiedColumnField>) (f : ComputedField) =
        let rec qualifyComputedExpr = function
            | FEValue(v) -> FEValue(v)
            | FEColumn({ entity = None; name = cname; }) ->
                match Map.tryFind cname columns with
                    | None -> raise <| QualifierError (sprintf "Column not found in computed field: %s" cname)
                    | Some(col) -> FEColumn(QFField(WrappedField(col.Field :> Field)))
            | FEColumn(_) -> raise <| QualifierError "Computed field expression cannot contain qualified field references"
            | FENot(a) -> FENot(qualifyComputedExpr a)
            | FEConcat(a, b) -> FEConcat(qualifyComputedExpr a, qualifyComputedExpr b)
            | FEEq(a, b) -> FEEq(qualifyComputedExpr a, qualifyComputedExpr b)
            | FEIn(a, bs) -> FEIn(qualifyComputedExpr a, Array.map qualifyComputedExpr bs)
            | FEAnd(a, b) -> FEAnd(qualifyComputedExpr a, qualifyComputedExpr b)

        let computedExpr =
            let lexbuf = LexBuffer<char>.FromString f.Expression
            try
                fieldExpr tokenstream lexbuf
            with
                | Failure(msg) -> raise <| QualifierError msg

        { field = WrappedField(f);
          expression = qualifyComputedExpr computedExpr;
        }

    and qualifyColumnField (f : ColumnField) =
        let fieldType =
            let lexbuf = LexBuffer<char>.FromString f.Type
            try
                fieldType tokenstream lexbuf
            with
                | Failure(msg) -> raise <| QualifierError msg

        { field = WrappedField(f);
          fieldType = qualifyFieldType fieldType;
        }

    and qualifyFieldType = function
        | FTInt -> FTInt
        | FTString -> FTString
        | FTBool -> FTBool
        | FTDateTime -> FTDateTime
        | FTDate -> FTDate
        | FTReference(e) ->
            let entity = getDbEntity e
            FTReference(entity)
        | FTEnum(vals) -> FTEnum(vals)

    and qualifyEntity (e : WrappedEntity) =
        let colFields = e.Entity.ColumnFields |> Seq.map (fun f -> (f.Name, qualifyColumnField f)) |> Map.ofSeq
        let compFields = e.Entity.ComputedFields |> Seq.map (fun f -> (f.Name, qualifyComputedField colFields f)) |> Map.ofSeq

        { entity = e;
          columnFields = colFields;
          computedFields = compFields;
        }

    and getDbEntity (e : EntityName) =
        let q = db.Entities
                    .Include(fun ent -> ent.Fields)
                    .Include(fun ent -> ent.Schema)
                    .Include(fun ent -> ent.SummaryField)
                    .Where(fun ent -> ent.Name = e.name)
        let res =
            match e.schema with
                | None -> q.SingleOrDefault(fun ent -> ent.Schema = null)
                | Some(schema) -> q.SingleOrDefault(fun ent -> ent.Schema.Name = schema)
        if res = null then
            raise <| QualifierError (sprintf "Entity not found: %s" e.name)
        else
            WrappedEntity(res)

    let lookupField (mapping : QMapping) (f : FieldName) =
        let entity =
            match f.entity with
                | None ->
                    if mapping.Count = 1 then
                        Map.toSeq mapping |> Seq.map snd |> Seq.head
                    else
                        raise <| QualifierError (sprintf "None or more than one possible interpretation: %s" f.name)
                | Some(ename) ->
                    match Map.tryFind ename mapping with
                        | None ->
                            raise <| QualifierError (sprintf "Field entity not found: %s" ename.name)
                        | Some(e) -> e
        match entity with
            // FIXME: improve error reporting
            | QMEntity(entity, fields) ->
                if f.name = "Id" then
                    QFEntityId(entity.entity)
                else
                    match Map.tryFind f.name fields with
                        | None ->
                            raise <| QualifierError (sprintf "Field not found: %s" f.name)
                        | Some(field) -> QFField(field)
            | QMSubquery(queryName, fields) ->
                if Set.contains f.name fields then
                    QFSubquery(queryName, f.name)
                else
                    raise <| QualifierError (sprintf "Field not found: %s" f.name)


    let rec qualifyQuery (entities : QEntities) query =
        let (qFrom, newMapping, newEntities) = qualifyFrom entities query.from

        let newQuery =
            { attributes = query.attributes;
              results = Array.map (fun (attr, res) -> (attr, qualifyResult newMapping res)) query.results;
              from = qFrom;
              where = Option.map (qualifyFieldExpr newMapping) query.where;
              orderBy = Array.map (fun (ord, expr) -> (ord, qualifyFieldExpr newMapping expr)) query.orderBy;
            }
        (newQuery, newEntities)

    and qualifyFrom (entities : QEntities) = function
        | FEntity(e) ->
            let (newEntities, entity, fields) =
                match Map.tryFind e entities with
                    | Some(entity) ->
                        // FIXME: possibly don't recompute this.
                        let fields = entity.Entity.Fields |> Seq.map (fun f -> (f.Name, WrappedField(f))) |> Map.ofSeq
                        (entities, entity, fields)
                    | None ->
                        let entity = e |> getDbEntity |> qualifyEntity
                        let fields = entity.Entity.Fields |> Seq.map (fun f -> (f.Name, WrappedField(f))) |> Map.ofSeq
                        (Map.add e entity entities, entity, fields)
            (FEntity(QEEntity(entity.entity)), mapSingleton e (QMEntity(entity, fields)), newEntities)
        | FJoin(jt, e1, e2, where) ->
            let (newE1, newMapping1, newEntities1) = qualifyFrom entities e1
            let (newE2, newMapping2, newEntities2) = qualifyFrom newEntities1 e2

            let newMapping =
                try
                    mapUnionUnique newMapping1 newMapping2
                with
                    | Failure(msg) -> raise <| QualifierError msg

            let newFieldExpr = qualifyFieldExpr newMapping where
            (FJoin(jt, newE1, newE2, newFieldExpr), newMapping, newEntities2)
        | FSubExpr(name, q) ->
            let (newQ, newEntities) = qualifyQuery entities q
            let fields = newQ.results |> Seq.map (fun (attr, res) -> resultName res) |> Set.ofSeq
            (FSubExpr(name, newQ), mapSingleton { schema = None; name = name; } (QMSubquery(name, fields)), newEntities)
                
    and qualifyFieldExpr mapping = function
        | FEValue(FNull) -> FEValue(FNull)
        | FEValue(v) -> FEValue(v)
        | FEColumn(f) -> FEColumn(lookupField mapping f)
        | FENot(a) -> FENot(qualifyFieldExpr mapping a)
        | FEConcat(a, b) -> FEConcat(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)
        | FEEq(a, b) -> FEEq(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)
        | FEIn(a, arr) -> FEIn(qualifyFieldExpr mapping a, Array.map (qualifyFieldExpr mapping) arr)
        | FEAnd(a, b) -> FEAnd(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)

    and qualifyResult mapping = function
        | RField(f) -> RField(lookupField mapping f)
        | RExpr(name, e) -> RExpr(name, qualifyFieldExpr mapping e)

    member this.QualifyQuery (query : ParsedQueryExpr) : QualifiedQuery =
        let (query, entities) = qualifyQuery Map.empty query
        { expression = query;
          entities = entities;
        }

    member this.QualifyType (ftype : ParsedFieldType) : QualifiedFieldType =
        qualifyFieldType ftype

    member this.QualifyEntity (entity : Entity) : QualifiedEntity =
        db.Entry(entity).Collection("Fields").Load()
        db.Entry(entity).Reference("Schema").Load()

        qualifyEntity (WrappedEntity(entity))

    static member QualifyDefaultExpr (entity : ParsedFieldExpr) : QualifiedFieldExpr =
        let rec qualifyDefaultExpr = function
            | FEValue(v) -> FEValue(v)
            | FEColumn(c) -> raise <| QualifierError "Column references are not supported in default values"
            | FENot(a) -> FENot(qualifyDefaultExpr a)
            | FEConcat(a, b) -> FEConcat(qualifyDefaultExpr a, qualifyDefaultExpr b)
            | FEEq(a, b) -> FEEq(qualifyDefaultExpr a, qualifyDefaultExpr b)
            | FEIn(a, arr) -> FEIn(qualifyDefaultExpr a, Array.map qualifyDefaultExpr arr)
            | FEAnd(a, b) -> FEAnd(qualifyDefaultExpr a, qualifyDefaultExpr b)
        qualifyDefaultExpr entity
