namespace FunWithFlags.FunDB.FunQL.Qualifier

open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser

exception QualifierError of string

module Name =
    let internal renderEntityName (e : Entity) =
        if e.SchemaId.HasValue then
            sprintf "%s.%s" (renderSqlName e.Schema.Name) (renderSqlName e.Name)
        else
            renderSqlName e.Name

    type QEntityName =
        internal
        | QEEntity of Entity
        | QESubquery of TableName
        with
            override this.ToString () =
                match this with
                    | QEEntity(e) -> renderEntityName e
                    | QESubquery(name) -> renderSqlName name

    // FIXME: allow non-qualified field names, like in SQL
    type QFieldName =
        | QFField of Field
        | QFEntityId of Entity
        | QFSubquery of TableName * ColumnName
        with
            override this.ToString () =
                match this with
                    | QFField(f) -> sprintf "%s.%s" (renderEntityName f.Entity) (renderSqlName f.Name)
                    | QFEntityId(e) -> sprintf "%s.\"Id\"" (renderEntityName e)
                    | QFSubquery(tableName, columnName) -> sprintf "%s.%s" (renderSqlName tableName) (renderSqlName columnName)

    let internal resultName = function
        | RField(QFField(field)) -> field.Name
        | RField(QFEntityId(entity)) -> "Id"
        | RField(QFSubquery(entityName, fieldName)) -> fieldName
        | RExpr(e, name) -> name

    type QDbEntityName =
        internal
        | QDEEntity of Entity
        with
            override this.ToString () =
                match this with
                    | QDEEntity(e) -> renderEntityName e

open Name

type QualifiedQueryExpr = QueryExpr<QEntityName, QFieldName>

type QualifiedFieldType = FieldType<QDbEntityName>

type private QMappedEntity =
    | QMEntity of Entity * Map<ColumnName, Field>
    | QMSubquery of TableName * Set<ColumnName>

type private QMapping = Map<EntityName, QMappedEntity>

type Qualifier internal (db : DatabaseContext) =
    let mappedToName = function
        | QMEntity(e, _) -> QEEntity(e)
        | QMSubquery(name, _) -> QESubquery(name)

    let getDbEntity (e : EntityName) =
        let q = db.Entities
                    .Include(fun ent -> ent.Fields)
                    .Include(fun ent -> ent.Schema)
                    .Where(fun ent -> ent.Name = e.name)
        let res =
            match e.schema with
                | None -> q.SingleOrDefault(fun ent -> ent.Schema = null)
                | Some(schema) -> q.SingleOrDefault(fun ent -> ent.Schema.Name = schema)
        if res = null then
            raise <| QualifierError (sprintf "Entity not found: %s" e.name)
        else
            (res, res.Fields |> Seq.map (fun f -> (f.Name, f)) |> Map.ofSeq)

    let lookupDbEntity mapping ename =
        match Map.tryFind ename mapping with
            | Some(QMEntity(entity, fields)) -> (entity, fields)
            | _ -> getDbEntity ename

    let lookupField (mapping : QMapping) f =
        let entity =
            match f.entity with
                | None ->
                    if mapping.Count = 1 then
                        Map.toSeq mapping |> Seq.map snd |> Seq.head
                    else
                        raise <| QualifierError (sprintf "None or more than one possible interpretation: %s" f.name)
                | Some(ename) ->
                    match Map.tryFind ename mapping with
                        | None -> raise <| QualifierError (sprintf "Field entity not found: %s" ename.name)
                        | Some(e) -> e
        match entity with
            // FIXME: improve error reporting
            | QMEntity(entity, fields) ->
                if f.name = "Id" then
                    QFEntityId(entity)
                else
                    match Map.tryFind f.name fields with
                        | None -> raise <| QualifierError (sprintf "Field not found: %s" f.name)
                        | Some(field) -> QFField(field)
            | QMSubquery(queryName, fields) ->
                if Set.contains f.name fields then
                    QFSubquery(queryName, f.name)
                else
                    raise <| QualifierError (sprintf "Field not found: %s" f.name)


    let rec qualifyQuery mapping query =
        let (newMapping, qFrom) = qualifyFrom mapping query.from

        { attributes = query.attributes;
          results = Array.map (fun (res, attr) -> (qualifyResult newMapping res, attr)) query.results;
          from = qFrom;
          where = Option.map (qualifyValueExpr newMapping) query.where;
          orderBy = Array.map (fun (field, ord) -> (lookupField newMapping field, ord)) query.orderBy;
        }

    and qualifyFrom mapping = function
        | FEntity(e) ->
            let (entity, fields) = lookupDbEntity mapping e
            (Map.add e (QMEntity(entity, fields)) mapping, FEntity(QEEntity(entity)))
        | FJoin(jt, e1, e2, where) ->
            let (newMapping1, newE1) = qualifyFrom mapping e1
            let (newMapping2, newE2) = qualifyFrom newMapping1 e2
            let newValueExpr = qualifyValueExpr newMapping2 where
            (newMapping2, FJoin(jt, newE1, newE2, newValueExpr))
        | FSubExpr(q, name) ->
            let newQ = qualifyQuery mapping q
            let fields = newQ.results |> Seq.map (fun (res, attr) -> resultName res) |> Set.ofSeq
            (Map.add { schema = None; name = name; } (QMSubquery(name, fields)) mapping, FSubExpr(newQ, name))
                
    and qualifyValueExpr mapping = function
        | WColumn(f) -> WColumn(lookupField mapping f)
        | WValue(v) -> WValue(v)
        | WEq(a, b) -> WEq(qualifyValueExpr mapping a, qualifyValueExpr mapping b)
        | WAnd(a, b) -> WAnd(qualifyValueExpr mapping a, qualifyValueExpr mapping b)

    and qualifyResult mapping = function
        | RField(f) -> RField(lookupField mapping f)
        | RExpr(e, name) -> RExpr(qualifyValueExpr mapping e, name)

    and qualifyFieldType mapping = function
        | FTInt -> FTInt
        | FTString -> FTString
        | FTReference(e) ->
            let (entity, _) = lookupDbEntity mapping e
            FTReference(QDEEntity(entity))

    member this.QualifyQuery (query: ParsedQueryExpr) : QualifiedQueryExpr =
        qualifyQuery Map.empty query

    member this.QualifyType (ftype: ParsedFieldType) : QualifiedFieldType =
        qualifyFieldType Map.empty ftype
