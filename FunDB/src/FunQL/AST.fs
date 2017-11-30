module FunWithFlags.FunDB.FunQL.AST

open System
open System.Globalization
open System.Runtime.InteropServices

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.Utils

type ColumnName = string
type TableName = string

type EntityName =
    { schema: string option;
      name: TableName;
    } with
        override this.ToString () =
            match this.schema with
                | None -> renderSqlName this.name
                | Some(x) -> sprintf "%s.%s" (renderSqlName x) (renderSqlName this.name)

        static member FromEntity (entity : Entity) =
            { schema = if entity.SchemaId.HasValue then Some(entity.Schema.Name) else None;
              name = entity.Name;
            }

type FieldName =
    { entity: EntityName option;
      name: ColumnName;
    } with
        override this.ToString () =
            match this.entity with
                | None -> renderSqlName this.name
                | Some(entity) -> sprintf "%O.%s" entity (renderSqlName this.name)

        static member FromField (field : Field) =
            { entity = Some <| EntityName.FromEntity field.Entity;
              name = field.Name;
            }

[<CustomEquality; CustomComparison>]
type WrappedEntity =
    | WrappedEntity of Entity
    with
        member this.Entity =
            match this with
                | WrappedEntity(e) -> e

        member this.Name = EntityName.FromEntity this.Entity

        override this.ToString () = renderEntityName this.Entity

        override x.Equals (yobj) =
            match yobj with
                | :? WrappedEntity as y -> x.Entity.Id = y.Entity.Id
                | _ -> false

        override this.GetHashCode () = hash this.Entity.Id

        interface IComparable with
            member x.CompareTo (yobj) =
                match yobj with
                    | :? WrappedEntity as y -> compare x.Entity.Id y.Entity.Id
                    | _ -> invalidArg "yobj" "Cannot compare values of different types"

[<CustomEquality; CustomComparison>]
type WrappedField<'f> when 'f :> Field =
    | WrappedField of 'f
    with
        member this.Field =
            match this with
                | WrappedField(f) -> f

        member this.Entity = WrappedEntity(this.Field.Entity)

        member this.Name = FieldName.FromField this.Field

        override this.ToString () = renderFieldName this.Field

        override x.Equals (yobj) =
            match yobj with
                | :? WrappedField<'f> as y -> x.Field.Id = y.Field.Id
                | _ -> false

        override this.GetHashCode () = hash this.Field.Id

        interface IComparable with
            member x.CompareTo (yobj) =
                match yobj with
                    | :? WrappedField<'f> as y -> compare x.Field.Id y.Field.Id
                    | _ -> invalidArg "yobj" "Cannot compare values of different types"

type FieldType<'e> =
    | FTInt
    | FTString
    | FTBool
    | FTDateTime
    | FTDate
    | FTReference of 'e
    with
        override this.ToString () =
            match this with
                | FTInt -> "int"
                | FTString -> "string"
                | FTBool -> "bool"
                | FTDateTime -> "datetime"
                | FTDate -> "date"
                | FTReference(e) -> sprintf "reference %O" e

        member this.TryReference () =
            match this with
                | FTReference(e) -> e
                | _ -> Unchecked.defaultof<'e>

type FieldValue =
    | FInt of int
    | FString of string
    | FBool of bool
    | FDateTime of DateTime
    | FDate of DateTime
    | FNull
    with
        override this.ToString () =
            match this with
                | FInt(i) -> i.ToString ()
                | FString(s) -> renderSqlString s
                | FBool(b) -> renderBool b
                | FDateTime(dt) -> dt.ToString("O") |> renderSqlString
                | FDate(d) -> d.ToString("d", CultureInfo.InvariantCulture) |> renderSqlString
                | FNull -> "NULL"

type FieldExpr<'c> =
    | FEValue of FieldValue
    | FEColumn of 'c
    | FENot of FieldExpr<'c>
    | FEConcat of FieldExpr<'c> * FieldExpr<'c>
    | FEEq of FieldExpr<'c> * FieldExpr<'c>
    | FEIn of FieldExpr<'c> * (FieldExpr<'c> array)
    | FEAnd of FieldExpr<'c> * FieldExpr<'c>

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () =
            match this with
                | Asc -> "ASC"
                | Desc -> "DESC"

type Result<'f> =
    | RField of 'f
    | RExpr of FieldExpr<'f> * ColumnName

type JoinType =
    | Inner
    | Left
    | Right
    | Outer
    with
        override this.ToString () =
            match this with
                | Left -> "LEFT"
                | Right -> "RIGHT"
                | Inner -> "INNER"
                | Outer -> "OUTER"

type QueryExpr<'e, 'f> =
    { attributes: AttributeMap;
      results: (Result<'f> * AttributeMap) array;
      from: FromExpr<'e, 'f>;
      where: FieldExpr<'f> option;
      orderBy: (FieldExpr<'f> * SortOrder) array;
    } with
        static member Create
            (
                attributes,
                results,
                from,
                [<Optional; DefaultParameterValue(null)>] ?where,
                [<Optional; DefaultParameterValue(null)>] ?orderBy
            ) =
                { attributes = attributes;
                  results = results;
                  from = from;
                  where = where;
                  orderBy = defaultArg orderBy Array.empty;
                }
                
          member this.MergeResults additionalResults = { this with results = Array.append additionalResults this.results; }

          member this.MergeWhere additionalWhere =
              match this.where with
                  | None -> { this with where = Some(additionalWhere); }
                  | Some(w1) -> { this with where = Some(FEAnd(w1, additionalWhere)); }

          member this.MergeOrderBy additionalOrderBy = { this with orderBy = Array.append this.orderBy additionalOrderBy; }

and FromExpr<'e, 'f> =
    | FEntity of 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * FieldExpr<'f>
    | FSubExpr of QueryExpr<'e, 'f> * TableName


type ParsedQueryExpr = QueryExpr<EntityName, FieldName>

type ParsedFieldExpr = FieldExpr<FieldName>

type ParsedFieldType = FieldType<EntityName>

let rec internal fromExprContains (entity: 'e) = function
    | FEntity(e) -> entity = e
    | FJoin(_, a, b, _) -> fromExprContains entity a || fromExprContains entity b
    | FSubExpr(_, _) -> false
