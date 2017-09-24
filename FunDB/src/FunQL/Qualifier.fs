module FunWithFlags.FunDB.FunQL.Qualifier

open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunCore
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB

type QEntityName =
    | QEEntity of Entity * Map<ColumnName, Field>
    | QESubquery of TableName * Set<ColumnName>

// FIXME: allow non-qualified field names, like in SQL
type QFieldName =
    | QFField of Entity * Field
    | QFSubquery of TableName * ColumnName

type QualifiedQueryExpr = QueryExpr<QEntityName, QFieldName>

let resultName = function
    | RField(QFField(entity, field)) -> field.Name
    | RField(QFSubquery(entityName, fieldName)) -> fieldName
    | RExpr(e, name) -> name

exception QualifierError of string

type Qualifier(dbQuery : DatabaseHandle) =
    let getDbEntity (e : EntityName) =
        let q = dbQuery.Database.Entities
                    .Include(fun ent -> ent.Fields)
                    .Include(fun ent -> ent.Schema)
                    .Where(fun ent -> ent.Name = e.name)
        let res =
            match e.schema with
                | None -> q.SingleOrDefault(fun ent -> ent.Schema = null)
                | Some(schema) -> q.SingleOrDefault(fun ent -> ent.Schema.Name = schema)
        if res = null then
            raise (QualifierError (sprintf "Entity not found: %s" e.name))
        else
            QEEntity(res, res.Fields |> Seq.map (fun f -> (f.Name, f)) |> Map.ofSeq)

    let lookupEntity mapping f =
        match Map.tryFind f mapping with
            | Some(e) -> e
            | _ -> getDbEntity f

    let lookupDbEntity mapping ename =
        match Map.tryFind ename mapping with
            | Some(QEEntity(_, _) as entity) -> entity
            | _ -> getDbEntity ename

    let lookupField mapping f =
        match Map.tryFind f.entity mapping with
            // FIXME: improve error reporting
            | None -> raise (QualifierError (sprintf "Field entity not found: %s" f.entity.name))
            | Some(QEEntity(entity, fields)) ->
                match Map.tryFind f.name fields with
                    | None -> raise (QualifierError (sprintf "Field not found: %s" f.name))
                    | Some(field) -> QFField(entity, field)
            | Some(QESubquery(queryName, fields)) ->
                if Set.contains f.name fields then
                    QFSubquery(queryName, f.name)
                else
                    raise (QualifierError (sprintf "Field not found: %s" f.name))

    let rec qualifyQuery mapping query =
        let (newMapping, qFrom) = qualifyFrom mapping query.from;

        { results = List.map (fun (res, attr) -> (qualifyResult newMapping res, attr)) query.results;
          from = qFrom;
          where = Option.map (qualifyWhere newMapping) query.where;
          orderBy = List.map (fun (field, ord) -> (lookupField newMapping field, ord)) query.orderBy;
        }

    and qualifyFrom mapping = function
        | FEntity(e) ->
            let newE = lookupDbEntity mapping e
            (Map.add e newE mapping, FEntity(newE))
        | FJoin(jt, e1, e2, where) ->
            let (newMapping1, newE1) = qualifyFrom mapping e1
            let (newMapping2, newE2) = qualifyFrom newMapping1 e2
            let newWhere = qualifyWhere newMapping2 where
            (newMapping2, FJoin(jt, newE1, newE2, newWhere))
        | FSubExpr(q, name) ->
            let newQ = qualifyQuery mapping q
            let fields = newQ.results |> Seq.map (fun (res, attr) -> resultName res) |> Set.ofSeq
            (Map.add { schema = None; name = name; } (QESubquery(name, fields)) mapping, FSubExpr(newQ, name))
                
    and qualifyWhere mapping = function
        | WField(f) -> WField(lookupField mapping f)
        | WInt(i) -> WInt(i)
        | WFloat(f) -> WFloat(f)
        | WString(s) -> WString(s)
        | WBool(b) -> WBool(b)
        | WEq(a, b) -> WEq(qualifyWhere mapping a, qualifyWhere mapping b)

    and qualifyResult mapping = function
        | RField(f) -> RField(lookupField mapping f)
        | RExpr(e, name) -> RExpr(qualifyResultExpr mapping e, name)

    and qualifyResultExpr mapping = function
        | REField(f) -> REField(lookupField mapping f)

    member this.Qualify (query: ParsedQueryExpr) : QualifiedQueryExpr =
        qualifyQuery Map.empty query
