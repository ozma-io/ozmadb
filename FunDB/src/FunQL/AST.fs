namespace FunWithFlags.FunDB.FunQL.AST

open System.Runtime.InteropServices
open FunWithFlags.FunCore
open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.Escape

type ColumnName = string
type TableName = string

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () =
            match this with
                | Asc -> "ASC"
                | Desc -> "DESC"

type ResultExpr<'e, 'f> =
    | REField of 'f

type Result<'e, 'f> =
    | RField of 'f
    | RExpr of ResultExpr<'e, 'f> * ColumnName

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
    { results: (Result<'e, 'f> * AttributeMap) array;
      from: FromExpr<'e, 'f>;
      where: ValueExpr<'e, 'f> option;
      orderBy: ('f * SortOrder) array;
    } with
        static member Create
            (
                results,
                from,
                [<Optional; DefaultParameterValue(null)>] ?where,
                [<Optional; DefaultParameterValue(null)>] ?orderBy
            ) =
                { results = results;
                  from = from;
                  where = where;
                  orderBy = defaultArg orderBy Array.empty;
                }
                
          member this.MergeResults additionalResults = { this with results = Array.append additionalResults this.results; }

          member this.MergeWhere additionalWhere =
              match this.where with
                  | None -> { this with where = Some(additionalWhere); }
                  | Some(w1) -> { this with where = Some(WAnd(w1, additionalWhere)); }

          member this.MergeOrderBy additionalOrderBy = { this with orderBy = Array.append this.orderBy additionalOrderBy; }

and Value =
    | VInt of int
    | VFloat of double
    | VString of string
    | VBool of bool
    | VNull
    with
        override this.ToString () =
            match this with
                | VInt(i) -> i.ToString ()
                | VFloat(f) -> invalidOp "Not supported"
                | VString(s) -> renderSqlString s
                | VBool(b) -> renderBool b
                | VNull -> "NULL"

and ValueExpr<'e, 'f> =
    | WValue of Value
    | WField of 'f
    | WEq of ValueExpr<'e, 'f> * ValueExpr<'e, 'f>
    | WAnd of ValueExpr<'e, 'f> * ValueExpr<'e, 'f>

and FromExpr<'e, 'f> =
    | FEntity of 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * ValueExpr<'e, 'f>
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

// FIXME: allow non-qualified field names, like in SQL
type FieldName =
    { entity: EntityName option;
      name: ColumnName;
    } with
        override this.ToString () =
            match this.entity with
                | None -> renderSqlName this.name
                | Some(entity) -> sprintf "%s.%s" (entity.ToString ()) (renderSqlName this.name)

        static member FromField (field : Field) =
            { entity = Some <| EntityName.FromEntity field.Entity;
              name = field.Name;
            }

type ParsedQueryExpr = QueryExpr<EntityName, FieldName>

type FieldType<'e> =
    | FTInt
    | FTString
    | FTReference of 'e

type ParsedFieldType = FieldType<EntityName>
