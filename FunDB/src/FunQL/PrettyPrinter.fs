module FunWithFlags.FunDB.FunQL.PrettyPrinter

open YC.PrettyPrinter.Pretty
open YC.PrettyPrinter.Doc
open YC.PrettyPrinter.StructuredFormat

open FunWithFlags.FunDB.Attribute
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.FunQL.AST

let rec internal aboveTagListL tagger = function
    | [] -> emptyL
    | [x] -> x
    | x::xs -> tagger x @@ aboveTagListL tagger xs

let internal aboveCommaListL = aboveTagListL (fun prefixL -> prefixL >|< Text ",")
let internal aboveSemicolonListL = aboveTagListL (fun prefixL -> prefixL >|< Text ";")

let rec internal ppAttributeMap attrMap =
    let values = attrMap |> Seq.map (function | KeyValue(name, attr) -> wordL (renderSqlName name) ++ wordL "=" ++ ppAttribute attr >|< wordL ";") |> Seq.toList
    wordL "{" -- aboveListL values ^^ wordL "}"

and internal ppAttribute = function
    | ABool(b) -> wordL <| renderBool b
    // FIXME
    | AFloat(f) -> invalidOp "Not supported"
    | AInt(i) -> wordL <| i.ToString ()
    | AString(s) -> wordL <| renderSqlString s
    | AList(l) ->
        Array.toSeq l |> Seq.map ppAttribute |> Seq.toList |> commaListL |> squareBracketL
    | AAssoc(a) -> ppAttributeMap a

let internal ppMaybeAttributeMap (attrMap : AttributeMap) =
    if attrMap.Count = 0
    then emptyL
    else ppAttributeMap attrMap

let rec internal ppQuery query =
    let resultsList = query.results |> Array.toSeq |> Seq.map (fun (res, attr) -> ppResult res -- ppMaybeAttributeMap attr) |> Seq.toList
    let maybeWhere =
        match query.where with
            | None -> emptyL
            | Some(whereE) -> wordL "WHERE" @@- ppFieldExpr whereE

    let maybeOrderBy =
        if Array.isEmpty query.orderBy
        then
            emptyL
        else
            let orderByList = query.orderBy |> Array.toSeq |> Seq.map (fun (field, ord) -> wordL (field.ToString ()) ++ wordL (ord.ToString ())) |> Seq.toList
            wordL "ORDER BY" @@- aboveCommaListL orderByList
    
    ppMaybeAttributeMap query.attributes
        @@ wordL "SELECT"
        @@- aboveCommaListL resultsList
        @@ wordL "FROM"
        @@- ppFrom query.from
        @@ maybeWhere
        @@ maybeOrderBy

and internal ppFrom = function
    | FEntity(e) -> wordL <| e.ToString ()
    | FJoin(jt, e1, e2, where) -> ppFrom e1 @@ (wordL (jt.ToString ()) ++ wordL "JOIN") @@ ppFrom e2 @@ (wordL "ON" ++ ppFieldExpr where)
    | FSubExpr(q, name) -> bracketL (ppQuery q) @@ (wordL "AS" ++ wordL (name.ToString ()))

and internal ppFieldExpr = function
    | FEValue(v) -> wordL <| v.ToString ()
    | FEColumn(c) -> wordL <| c.ToString ()
    | FENot(a) -> wordL "NOT" ++ bracketL (ppFieldExpr a)
    | FEConcat(a, b) -> bracketL (ppFieldExpr a) ++ wordL "||" ++ bracketL (ppFieldExpr b)
    | FEEq(a, b) -> bracketL (ppFieldExpr a) ++ wordL "=" ++ bracketL (ppFieldExpr b)
    | FEIn(a, arr) -> bracketL (ppFieldExpr a) ++ wordL "IN" ++ bracketL (arr |> Seq.map ppFieldExpr |> Seq.toList |> commaListL)
    | FEAnd(a, b) -> bracketL (ppFieldExpr a) ++ wordL "AND" ++ bracketL (ppFieldExpr b)

and internal ppResult = function
    | RField(f) -> wordL <| f.ToString ()
    | RExpr(e, name) -> bracketL (ppFieldExpr e) @@ (wordL "AS" ++ wordL (name.ToString ()))

and internal ppFieldType = function
    | FTInt -> wordL "int"
    | FTString -> wordL "string"
    | FTBool -> wordL "bool"
    | FTDateTime -> wordL "datetime"
    | FTDate -> wordL "date"
    | FTReference(e) -> wordL "reference" ++ wordL (e.ToString ())

let QueryToString width q = print width <| ppQuery q

let FieldTypeToString width ft = print width <| ppFieldType ft
