namespace FunWithFlags.FunDB.FunQL.AST

open System
open System.Globalization
open System.Runtime.InteropServices

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.SQL.Utils

type ColumnName = string
type TableName = string

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

type Result<'e, 'f> =
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
      results: (Result<'e, 'f> * AttributeMap) array;
      from: FromExpr<'e, 'f>;
      where: FieldExpr<'f> option;
      orderBy: ('f * SortOrder) array;
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

type ParsedQueryExpr = QueryExpr<EntityName, FieldName>

type ParsedFieldExpr = FieldExpr<FieldName>

type ParsedFieldType = FieldType<EntityName>
