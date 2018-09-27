module FunWithFlags.FunDB.FunQL.AST

open System
open System.Globalization
open System.Runtime.InteropServices

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.Utils

type FunQLName = FunQLName of string
    with
        override this.ToString () =
            match this with
                | FunQLName name -> name

        member this.ToFunQLString () =
            match this with
                | FunQLName c -> renderSqlName c

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type SchemaName = FunQLName
type EntityName = FunQLName
type FieldName = FunQLName
type ConstraintName = FunQLName

type EntityRef =
    { schema : SchemaName option
      name : EntityName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.schema with
                | None -> this.name.ToFunQLString()
                | Some x -> sprintf "%s.%s" (x.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldRef =
    { entity : EntityRef option
      name : FieldName
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this.entity with
                | None -> this.name.ToFunQLString()
                | Some entity -> sprintf "%s.%s" (entity.ToFunQLString()) (this.name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldValue =
    | FInt of int
    | FString of string
    | FBool of bool
    | FDateTime of DateTime
    | FDate of DateTime
    | FIntArray of int array
    | FStringArray of string array
    | FBoolArray of bool array
    | FDateTimeArray of DateTime array
    | FDateArray of DateTime array
    | FNull
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let renderArray func typeName vals = sprintf "%s :: array(%s)" (vals |> Seq.map func |> String.concat ", " |> sprintf "{%s}" |> renderSqlString) typeName
            match this with
                | FInt i -> renderSqlInt i
                | FString s -> renderSqlString s
                | FBool b -> renderBool b
                | FDateTime dt -> sprintf "%s :: datetime" (dt |> renderSqlDateTime |> renderSqlString)
                | FDate d -> sprintf "%s :: date" (d |> renderSqlDate |> renderSqlString)
                | FIntArray vals -> renderArray renderSqlInt "int" vals
                | FStringArray vals -> renderArray escapeDoubleQuotes "string" vals
                | FBoolArray vals -> renderArray renderSqlBool "bool" vals
                | FDateTimeArray vals -> renderArray (renderSqlDateTime >> escapeDoubleQuotes) "datetime" vals
                | FDateArray vals -> renderArray (renderSqlDate >> escapeDoubleQuotes) "date" vals
                | FNull -> "NULL"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type ScalarFieldType =
    | SFTInt
    | SFTString
    | SFTBool
    | SFTDateTime
    | SFTDate
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | SFTInt -> "int"
                | SFTString -> "string"
                | SFTBool -> "bool"
                | SFTDateTime -> "datetime"
                | SFTDate -> "date"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldExprType =
    | FETScalar of ScalarFieldType
    | FETArray of ScalarFieldType
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | FETScalar s -> s.ToFunQLString()
                | FETArray valType -> sprintf "array(%s)" (valType.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldType<'e, 'f> =
    | FTType of FieldExprType
    | FTReference of 'e * FieldExpr<'f> option
    | FTEnum of Set<string>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | FTType t -> t.ToFunQLString()
                | FTReference e None -> sprintf "reference(%s)" (e.ToFunQLString())
                | FTReference e (Some check) -> sprintf "reference(%s, %s)" (e.ToFunQLString()) (check.ToFunQLString())
                | FTEnum vals -> sprintf "enum(%s)" (vals |> Seq.map (fun x -> sprintf "\"%s\"" (renderSqlString x)) |> String.concat ", ")

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FieldExpr<'c> =
    | FEValue of FieldValue
    | FEColumn of 'c
    | FEPlaceholder of string
    | FENot of FieldExpr<'c>
    | FEAnd of FieldExpr<'c> * FieldExpr<'c>
    | FEOr of FieldExpr<'c> * FieldExpr<'c>
    | FEConcat of FieldExpr<'c> * FieldExpr<'c>
    | FEEq of FieldExpr<'c> * FieldExpr<'c>
    | FENotEq of FieldExpr<'c> * FieldExpr<'c>
    | FELike of FieldExpr<'c> * FieldExpr<'c>
    | FENotLike of FieldExpr<'c> * FieldExpr<'c>
    | FELess of FieldExpr<'c> * FieldExpr<'c>
    | FELessEq of FieldExpr<'c> * FieldExpr<'c>
    | FEGreater of FieldExpr<'c> * FieldExpr<'c>
    | FEGreaterEq of FieldExpr<'c> * FieldExpr<'c>
    | FEIn of FieldExpr<'c> * (FieldExpr<'c> array)
    | FENotIn of FieldExpr<'c> * (FieldExpr<'c> array)
    | FEIsNull of FieldExpr<'c>
    | FEIsNotNull of FieldExpr<'c>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | FEValue value -> value.ToFunQLString()
                | FEColumn c -> c.ToFunQLString()
                | FEPlaceholder s -> sprintf "$%s" s
                | FENot e -> sprintf "NOT (%s)" (e.ToFunQLString())
                | FEAnd (a, b) -> sprintf "(%s) AND (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEOr (a, b) -> sprintf "(%s) OR (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEConcat (a, b) -> sprintf "(%s) || (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEEq (a, b) -> sprintf "(%s) = (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FENotEq (a, b) -> sprintf "(%s) <> (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FELike (e, pat) -> sprintf "(%s) LIKE (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
                | FENotLike (e, pat) -> sprintf "(%s) NOT LIKE (%s)" (e.ToFunQLString()) (pat.ToFunQLString())
                | FELess (a, b) -> sprintf "(%s) < (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FELessEq (a, b) -> sprintf "(%s) <= (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEGreater (a, b) -> sprintf "(%s) > (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEGreaterEq (a, b) -> sprintf "(%s) >= (%s)" (a.ToFunQLString()) (b.ToFunQLString())
                | FEIn (e, vals) ->
                    assert (not <| Array.isEmpty vals)
                    sprintf "(%s) IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
                | FENotIn (e, vals) ->
                    assert (not <| Array.isEmpty vals)
                    sprintf "(%s) NOT IN (%s)" (e.ToFunQLString()) (vals |> Seq.map (fun v -> v.ToFunQLString()) |> String.concat ", ")
                | FEIsNull e -> sprintf "(%s) IS NULL" (e.ToFunQLString())
                | FEIsNotNull e -> sprintf "(%s) IS NOT NULL" (e.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

let mapFieldExpr (colFunc : 'a -> 'b) (placeholderFunc : string -> string) : FieldExpr<'a> -> FieldExpr<'b> =
    let rec traverse = function
        | FEValue value -> FEValue value
        | FEColumn c -> FEColumn (columnFunc c)
        | FEPlaceholder s -> FEPlaceholder (placeholderFunc s)
        | FENot e -> FENot (traverse e)
        | FEAnd (a, b) -> FEAnd (traverse a, traverse b)
        | FEOr (a, b) -> FEOr (traverse a, traverse b)
        | FEConcat (a, b) -> FEConcat (traverse a, traverse b)
        | FEEq (a, b) -> FEEq (traverse a, traverse b)
        | FENotEq (a, b) -> FENotEq (traverse a, traverse b)
        | FELike (e, pat) -> FELike (traverse e, traverse pat)
        | FENotLike (e, pat) -> FENotLike (traverse e, traverse pat)
        | FELess (a, b) -> FELess (traverse a, traverse b)
        | FELessEq (a, b) -> FELessEq (traverse a, traverse b)
        | FEGreater (a, b) -> FEGreater (traverse a, traverse b)
        | FEGreaterEq (a, b) -> FEGreaterEq (traverse a, traverse b)
        | FEIn (e, vals) -> FEIn (traverse e, Array.map traverse vals)
        | FENotIn (e, vals) -> FENotIn (traverse e, Array.map traverse vals)
        | FEIsNull e -> FEIsNull (traverse e)
        | FEIsNotNull e -> FEIsNotNull (traverse e)
    traverse

let iterFieldExpr (colFunc : 'a -> unit) (placeholderFunc : string -> unit) : FieldExpr<'a> -> unit =
    let rec traverse = function
        | FEValue value -> ()
        | FEColumn c -> colFunc c
        | FEPlaceholder s -> placeholderFunc s
        | FENot e -> traverse e
        | FEAnd (a, b) -> traverse a; traverse b
        | FEOr (a, b) -> traverse a; traverse b
        | FEConcat (a, b) -> traverse a; traverse b
        | FEEq (a, b) -> traverse a; traverse b
        | FENotEq (a, b) -> traverse a; traverse b
        | FELike (e, pat) -> traverse e; traverse pat
        | FENotLike (e, pat) -> traverse e; traverse pat
        | FELess (a, b) -> traverse a; traverse b
        | FELessEq (a, b) -> traverse a; traverse b
        | FEGreater (a, b) -> traverse a; traverse b
        | FEGreaterEq (a, b) -> traverse a; traverse b
        | FEIn (e, vals) -> traverse e; Array.iter traverse vals
        | FENotIn (e, vals) -> traverse e; Array.iter traverse vals
        | FEIsNull e -> traverse e
        | FEIsNotNull e -> traverse e
    traverse

type SortOrder =
    | Asc
    | Desc
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | Asc -> "ASC"
                | Desc -> "DESC"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type QueryResult<'f> =
    { name : FunQLName
      expr : FieldExpr<'f>
    }
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () = sprintf "%s AS %s" (expr.ToFunQLString()) (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type JoinType =
    | Inner
    | Left
    | Right
    | Outer
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | Left -> "LEFT"
                | Right -> "RIGHT"
                | Inner -> "INNER"
                | Outer -> "OUTER"

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

type FromClause<'e, 'f> =
    { from: FromExpr<'e, 'f>
      where: FieldExpr<'f> option
      orderBy: (SortOrder * FieldExpr<'f>) array
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let whereStr =
                match this.where with
                    | None -> ""
                    | Some cond -> sprintf "WHERE %s" (cond.ToFunQLString())
            let orderByStr =
                if this.orderBy.Count = 0
                then ""
                else sprintf "ORDER BY %s" (this.orderBy |> Seq.map (fun (ord, expr) -> sprintf "%s %s" (ord.ToFunQLString()) (expr.ToFunQLString())) |> String.concat ", ")
            sprintf "FROM %s" (concatWithWhitespaces [this.from.ToFunQLString(), whereStr, orderByStr])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString()

and QueryExpr<'e, 'f> =
    { results: QueryResult<'f> array
      clause: FromClause<'e, 'f>
    } with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            let resultsStr = this.results |> Seq.map (fun res -> res.ToFunQLString()) |> String.concat ", "
            sprintf "SELECT %s" (concatWithWhitespaces [resultsStr, clause.ToFunQLString()])

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

and FromExpr<'e, 'f> =
    | FEntity of 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * FieldExpr<'f>
    | FSubExpr of EntityName * QueryExpr<'e, 'f>
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | FEntity e -> e.ToFunQLString()
                | FJoin (joinType, a, b, cond) -> sprintf "%s %s JOIN %s ON %s" (a.ToFunQLString()) (joinType.ToFunQLString()) (b.ToFunQLString()) (cond.ToFunQLString())
                | FSubExpr (name, expr) -> sprintf "(%s) AS %s" (expr.ToFunQLString()) (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString () = this.ToFunQLString ()

type AttributeMap<'f> = Map<FunQLName, FieldExpr<'f>>

type ParsedFieldType = FieldType<EntityRef, FieldRef>

type ViewExpr<'f> =
    { arguments : Map<FunQLName, ParsedFieldType>
      attributes : AttributeMap<'f>
      results : (AttributeMap<'f> * QueryResult<'f>) array
      clause : FromClause<EntityRef, 'f>
    }

let funId = FunQLName "Id"
let funSubEntity = FunQLName "SubEntity"
