namespace FunWithFlags.FunDB.FunQL.Qualifier

open System
open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser

exception QualifierError of string

module Name =
    let internal renderEntityName (e : Entity) =
        if e.SchemaId.HasValue then
            sprintf "%s.%s" (renderSqlName e.Schema.Name) (renderSqlName e.Name)
        else
            renderSqlName e.Name

    [<CustomEquality; CustomComparison>]
    type QEntityName =
        internal
        | QEEntity of Entity
        | QESubquery of TableName
        with
            override this.ToString () =
                match this with
                    | QEEntity(e) -> renderEntityName e
                    | QESubquery(name) -> renderSqlName name

            member inline private this.ConstructorId =
                match this with
                    | QEEntity(_) -> 0
                    | QESubquery(_) -> 1

            override x.Equals (yobj) =
                match yobj with
                    | :? QEntityName as y ->
                        match (x, y) with
                            | (QEEntity(a), QEEntity(b)) -> a.Id = b.Id
                            | (QESubquery(a), QESubquery(b)) -> a = b
                            | _ -> false
                    | _ -> false

            override this.GetHashCode () =
                match this with
                    | QEEntity(e) -> combineHash this.ConstructorId (hash e.Id)
                    | QESubquery(name) -> combineHash this.ConstructorId (hash name)
                   
            interface IComparable with
                member x.CompareTo (yobj) =
                    match yobj with
                        | :? QEntityName as y ->
                            match (x, y) with
                                | (QEEntity(a), QEEntity(b)) -> compare a.Id b.Id
                                | (QESubquery(a), QESubquery(b)) -> compare a b
                                | (a, b) -> compare a.ConstructorId b.ConstructorId
                        | _ -> invalidArg "yobj" "Cannot compare values of different types"

    // FIXME: allow non-qualified field names, like in SQL
    [<CustomEquality; CustomComparison>]
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

            member inline private this.ConstructorId =
                match this with
                    | QFField(_) -> 0
                    | QFEntityId(_) -> 1
                    | QFSubquery(_) -> 2

            override x.Equals (yobj) =
                match yobj with
                    | :? QFieldName as y ->
                        match (x, y) with
                            | (QFField(a), QFField(b)) -> a.Id = b.Id
                            | (QFEntityId(a), QFEntityId(b)) -> a.Id = b.Id
                            | (QFSubquery(aTname, aCname), QFSubquery(bTname, bCname)) -> aTname = bTname && aCname = bCname
                            | _ -> false
                    | _ -> false

            override this.GetHashCode () =
                match this with
                    | QFField(f) -> combineHash this.ConstructorId (hash f.Id)
                    | QFEntityId(e) -> combineHash this.ConstructorId (hash e.Id)
                    | QFSubquery(tname, cname) -> combineHash this.ConstructorId (combineHash (hash tname) (hash cname))
                   
            interface IComparable with
                member x.CompareTo (yobj) =
                    match yobj with
                        | :? QFieldName as y ->
                            match (x, y) with
                                | (QFField(a), QFField(b)) -> compare a.Id b.Id
                                | (QFEntityId(a), QFEntityId(b)) -> compare a.Id b.Id
                                | (QFSubquery(aTname, aCname), QFSubquery(bTname, bCname)) -> combineCompare (compare aTname bTname) (compare aCname bCname)
                                | (a, b) -> compare a.ConstructorId b.ConstructorId
                        | _ -> invalidArg "yobj" "Cannot compare values of different types"

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

type QualifiedResult = Result<QEntityName, QFieldName>

type QualifiedQueryExpr = QueryExpr<QEntityName, QFieldName>

type QualifiedFromExpr = FromExpr<QEntityName, QFieldName>

type QualifiedFieldType = FieldType<QDbEntityName>

type QualifiedFieldExpr = FieldExpr<QFieldName>

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
            eprintfn "Entity not found: %s" e.name
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
          where = Option.map (qualifyFieldExpr newMapping) query.where;
          orderBy = Array.map (fun (field, ord) -> (lookupField newMapping field, ord)) query.orderBy;
        }

    and qualifyFrom mapping = function
        | FEntity(e) ->
            let (entity, fields) = lookupDbEntity mapping e
            (Map.add e (QMEntity(entity, fields)) mapping, FEntity(QEEntity(entity)))
        | FJoin(jt, e1, e2, where) ->
            let (newMapping1, newE1) = qualifyFrom mapping e1
            let (newMapping2, newE2) = qualifyFrom newMapping1 e2
            let newFieldExpr = qualifyFieldExpr newMapping2 where
            (newMapping2, FJoin(jt, newE1, newE2, newFieldExpr))
        | FSubExpr(q, name) ->
            let newQ = qualifyQuery mapping q
            let fields = newQ.results |> Seq.map (fun (res, attr) -> resultName res) |> Set.ofSeq
            (Map.add { schema = None; name = name; } (QMSubquery(name, fields)) mapping, FSubExpr(newQ, name))
                
    and qualifyFieldExpr mapping = function
        | FEValue(v) -> FEValue(v)
        | FEColumn(f) -> FEColumn(lookupField mapping f)
        | FENot(a) -> FENot(qualifyFieldExpr mapping a)
        | FEConcat(a, b) -> FEConcat(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)
        | FEEq(a, b) -> FEEq(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)
        | FEIn(a, arr) -> FEIn(qualifyFieldExpr mapping a, Array.map (qualifyFieldExpr mapping) arr)
        | FEAnd(a, b) -> FEAnd(qualifyFieldExpr mapping a, qualifyFieldExpr mapping b)

    and qualifyResult mapping = function
        | RField(f) -> RField(lookupField mapping f)
        | RExpr(e, name) -> RExpr(qualifyFieldExpr mapping e, name)

    and qualifyFieldType mapping = function
        | FTInt -> FTInt
        | FTString -> FTString
        | FTBool -> FTBool
        | FTDateTime -> FTDateTime
        | FTDate -> FTDate
        | FTReference(e) ->
            let (entity, _) = lookupDbEntity mapping e
            FTReference(QDEEntity(entity))

    member this.QualifyQuery (query: ParsedQueryExpr) : QualifiedQueryExpr =
        qualifyQuery Map.empty query

    member this.QualifyType (ftype: ParsedFieldType) : QualifiedFieldType =
        qualifyFieldType Map.empty ftype
