module FunWithFlags.FunDB.FunQL.Compiler

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL

type CompiledArgument =
    { placeholder : int
      fieldType : ParsedFieldType
      valueType : AST.SimpleValueType
    }

type CompiledViewExpr =
    { // Evaluation of column-wise or global attributes
      attributesStr : string
      query : AST.SelectExpr
      arguments : Map<string, CompiledArgument>
    } with
        override this.ToString () = sprintf "%s; %s" this.attributesStr (this.query.ToString())

let private compileScalarType : ScalarFieldType -> AST.SimpleType = function
    | SFTInt -> AST.STInt
    | SFTString -> AST.STString
    | SFTBool -> AST.STBool
    | SFTDateTime -> AST.STDateTime
    | SFTDate -> AST.STDate

let private compileFieldExprType : FieldExprType -> AST.SimpleValueType = function
    | FETScalar stype -> AST.VTScalar <| compileScalarType stype
    | FETArray stype -> AST.VTArray <| compileScalarType stype

let compileFieldType : FieldType<EntityRef, _> -> AST.SimpleValueType = function
    | FTType fetype -> compileFieldExprType fetype
    | FTReference (ent, restriction) -> AST.VTScalar AST.STInt
    | FTEnum vals -> AST.VTScalar AST.STString

let private compileOrder : SortOrder -> AST.SortOrder = function
    | Asc -> AST.Asc
    | Desc -> AST.Desc

let private compileJoin : JoinType -> AST.JoinType = function
    | Left -> AST.Left
    | Right -> AST.Right
    | Inner -> AST.Inner
    | Outer -> AST.Full

let private checkPureExpr (expr : FieldExpr<_>) : bool =
    let mutable noReferences = true
    let foundReference column =
        noReferences <- false
    iterFieldExpr foundReference ignore expr
    noReferences

let private compileName : FunQLName -> AST.SQLName = function
    | FunQLName name -> AST.SQLName name

let private compileEntityPun (entityRef : EntityRef) : AST.TableName =
    match entityRef.schema with
        | None -> compileName entityRef.name
        | Some schemaName -> AST.SQLName <| sprintf "%O__%O" schemaName entityRef.name

let private compileEntityTablePun (entityRef : EntityRef) : AST.TableRef =
    { schema = None; name = compileEntityPun entityRef }

let compileEntityRef (entityRef : EntityRef) : AST.TableRef = { schema = Option.map compileName entityRef.schema; name = compileName entityRef.name }

let compileFieldRef (fieldRef : ResolvedFieldRef) : AST.ColumnRef =
    { table = Some <| compileEntityRef fieldRef.entity; name = compileName fieldRef.name }

let private compileFieldName : ResolvedFieldName -> AST.ColumnRef = function
    | RFField fieldRef -> { table = Some <| compileEntityTablePun fieldRef.entity; name = compileName fieldRef.name }
    | RFSubquery (entityName, fieldName, boundField) -> { table = Some { schema = None; name = compileName entityName }; name = compileName fieldName }

let compileArray (vals : 'a array) : AST.ValueArray<'a> = Array.map AST.AVValue vals

let private compileFieldValue : FieldValue -> AST.ValueExpr<_> = function
    | FInt i -> AST.VEValue (AST.VInt i)
    // PostgreSQL cannot deduce text's type on its own
    | FString s -> AST.VECast (AST.VEValue (AST.VString s), AST.VTScalar (AST.STString.ToSQLName()))
    | FBool b -> AST.VEValue (AST.VBool b)
    | FDateTime dt -> AST.VEValue (AST.VDateTime dt)
    | FDate d -> AST.VEValue (AST.VDate d)
    | FIntArray vals -> AST.VEValue (AST.VIntArray (compileArray vals))
    | FStringArray vals -> AST.VEValue (AST.VStringArray (compileArray vals))
    | FBoolArray vals -> AST.VEValue (AST.VBoolArray (compileArray vals))
    | FDateTimeArray vals -> AST.VEValue (AST.VDateTimeArray (compileArray vals))
    | FDateArray vals -> AST.VEValue (AST.VDateArray (compileArray vals))
    | FNull -> AST.VEValue AST.VNull

let genericCompileFieldExpr (columnFunc : 'c -> 'f) (placeholderFunc : string -> int) : FieldExpr<'c> -> AST.ValueExpr<'f> =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FEColumn c -> AST.VEColumn (columnFunc c)
        | FEPlaceholder name -> AST.VEPlaceholder (placeholderFunc name)
        | FENot a -> AST.VENot (traverse a)
        | FEAnd (a, b) -> AST.VEAnd (traverse a, traverse b)
        | FEOr (a, b) -> AST.VEOr (traverse a, traverse b)
        | FEConcat (a, b) -> AST.VEConcat (traverse a, traverse b)
        | FEEq (a, b) -> AST.VEEq (traverse a, traverse b)
        | FENotEq (a, b) -> AST.VENotEq (traverse a, traverse b)
        | FELike (e, pat) -> AST.VELike (traverse e, traverse pat)
        | FENotLike (e, pat) -> AST.VENotLike (traverse e, traverse pat)
        | FELess (a, b) -> AST.VELess (traverse a, traverse b)
        | FELessEq (a, b) -> AST.VELessEq (traverse a, traverse b)
        | FEGreater (a, b) -> AST.VEGreater (traverse a, traverse b)
        | FEGreaterEq (a, b) -> AST.VEGreaterEq (traverse a, traverse b)
        | FEIn (a, arr) -> AST.VEIn (traverse a, Array.map traverse arr)
        | FENotIn (a, arr) -> AST.VENotIn (traverse a, Array.map traverse arr)
        | FECast (e, typ) -> AST.VECast (traverse e, AST.mapValueType (fun (x : AST.SimpleType) -> x.ToSQLName()) (compileFieldExprType typ))
        | FEIsNull a -> AST.VEIsNull (traverse a)
        | FEIsNotNull a -> AST.VEIsNotNull (traverse a)
    traverse

type private QueryCompiler (layout : Layout, placeholders : Map<string, int>) =
    let compileFieldExpr = genericCompileFieldExpr compileFieldName (fun name -> placeholders.[name])
    let compileLocalFieldExpr (tableRef : AST.TableRef option) : LocalFieldExpr -> AST.FullValueExpr =
        let makeFullName name = { table = tableRef; name = compileName name } : AST.ColumnRef
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
        genericCompileFieldExpr makeFullName voidPlaceholder

    let compileAttribute prefix (FunQLName name) expr = AST.SCExpr (AST.SQLName <| sprintf "__%s__%s" prefix name, compileFieldExpr expr)

    let compileResult (result : ResolvedQueryResult) =
        match result.expression with
            | FEColumn c -> AST.SCColumn (compileFieldName c)
            | _ -> AST.SCExpr (compileName result.name, compileFieldExpr result.expression)

    let findReference (name : FieldName) (field : ResolvedColumnField) : ResolvedFieldRef option =
        match field.fieldType with
            | FTReference (entityRef, where) ->
                let entity = Option.get <| layout.FindEntity(entityRef)
                Some { entity = entityRef; name = entity.mainField }
            | _ -> None

    let rec compileFrom : ResolvedFromExpr -> AST.FromExpr = function
        | FEntity entityRef ->
            let entity = Option.get <| layout.FindEntity(entityRef)
            let tableRef = compileEntityRef entityRef
            let idColumn = AST.SCColumn { table = Some tableRef; name = AST.SQLName "Id" }

            let columnFields = entity.columnFields |> Map.toSeq |> Seq.map (fun (name, field) -> AST.SCColumn { table = Some tableRef; name = compileName name })
            let computedFields = entity.computedFields |> Map.toSeq |> Seq.map (fun (name, field) -> AST.SCExpr (compileName name, compileLocalFieldExpr (Some tableRef) field.expression))
            let references = Map.update findReference entity.columnFields

            let buildReferences (from : AST.FromExpr) (fieldName : FieldName) (referenceRef : ResolvedFieldRef) =
                let columnName = compileName fieldName
                let table = compileEntityRef referenceRef.entity
                let where = AST.VEEq (AST.VEColumn { table = Some tableRef; name = columnName }, AST.VEColumn { table = Some table; name = AST.SQLName "Id" }) : AST.FullValueExpr
                AST.FJoin (AST.Left, from, AST.FTable table, where)
            let from = Map.fold buildReferences (AST.FTable tableRef) references

            let makeReferenceName (fieldName : FieldName, referenceRef : ResolvedFieldRef) =
                AST.SCExpr (AST.SQLName <| sprintf "__Name__%O" fieldName, AST.VEColumn <| compileFieldRef referenceRef)
            let referenceNames = references |> Map.toSeq |> Seq.map makeReferenceName

            let subquery =
                { columns = Seq.append (Seq.append (Seq.singleton idColumn) (Seq.append columnFields computedFields)) referenceNames |> Seq.toArray 
                  clause =
                      Some { from = from
                             where = None
                             orderBy = [||]
                             limit = None
                             offset = None
                           }
                } : AST.SelectExpr
            
            AST.FSubExpr (compileEntityPun entityRef, subquery)
        | FJoin (jt, e1, e2, where) -> AST.FJoin (compileJoin jt, compileFrom e1, compileFrom e2, compileFieldExpr where)
        // FIXME: track field names across subexpressions
        | FSubExpr (name, q) -> AST.FSubExpr (compileName name, compileQueryExpr q)

    and compileFromClause (clause : ResolvedFromClause) : AST.FromClause =
        { from = compileFrom clause.from
          where = Option.map compileFieldExpr clause.where
          orderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr expr)) clause.orderBy
          // FIXME: support them!
          limit = None
          offset = None
        }

    and compileQueryExpr (query : ResolvedQueryExpr) : AST.SelectExpr =
        { columns = Array.map compileResult query.results
          clause = Some <| compileFromClause query.clause
        }

    member this.CompileFromClause = compileFromClause
    member this.CompileAttribute = compileAttribute
    member this.CompileResult = compileResult

let compileViewExpr (layout : Layout) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let arguments =
        viewExpr.arguments
            |> Map.toSeq
            |> Seq.mapi (fun i (name, fieldType) -> (name, ({ placeholder = i; fieldType = fieldType; valueType = compileFieldType fieldType })))
            |> Map.ofSeq
    let placeholders = Map.map (fun name arg -> arg.placeholder) arguments
    let compiler = QueryCompiler (layout, placeholders)

    let (queryAttributes, rowAttributes) =
        viewExpr.attributes
            |> Map.toArray
            |> Array.map (fun (name, expr) -> (expr, compiler.CompileAttribute "Row" name expr))
            |> Array.partition (fun (expr, col) -> checkPureExpr expr)
    let (columnAttributes, cellAttributes) =
        viewExpr.results
            |> Array.map (fun (attrs, result) -> attrs |> Map.toArray |> Array.map (fun (name, expr) -> (expr, compiler.CompileAttribute (sprintf "Cell__%O" result.name) name expr)))
            |> Array.concat
            |> Array.partition (fun (expr, col) -> checkPureExpr expr)
        
    let attributesQuery =
        { columns = Array.append (Array.map snd queryAttributes) (Array.map snd columnAttributes)
          clause = None
        } : AST.SelectExpr

    let resultColumns = Array.map (fun (attr, res) -> compiler.CompileResult res) viewExpr.results
    let getNameAttribute (attributes, result : ResolvedQueryResult) =
        match result.expression with
            | FEColumn (RFField fieldRef) when fieldRef.name <> funId ->
                let field = Option.get <| layout.FindField fieldRef.entity fieldRef.name
                match field with
                    | RColumnField { fieldType = FTReference (entityRef, where) } ->
                        let nameColumn = { table = Some <| compileEntityTablePun fieldRef.entity; name = AST.SQLName <| sprintf "__Name__%O" fieldRef.name } : AST.ColumnRef
                        Some <| AST.SCExpr (AST.SQLName <| sprintf "__Name__%O" result.name, AST.VEColumn nameColumn)
                    | _ -> None
            | _ -> None
    let nameAttributes =
        viewExpr.results
        |> Seq.mapMaybe getNameAttribute
        |> Array.ofSeq

    let idColumn =
        match viewExpr.update with
            | None -> [||]
            | Some updateExpr ->
                [| AST.SCExpr (AST.SQLName "__Id", AST.VEColumn { table = Some <| compileEntityTablePun updateExpr.entity; name = AST.SQLName "Id" }) |]

    let queryClause = compiler.CompileFromClause viewExpr.clause
    let query =
        { columns = Array.concat [ idColumn; Array.map snd rowAttributes; Array.map snd cellAttributes; resultColumns; nameAttributes ]
          clause = Some queryClause
        } : AST.SelectExpr

    { attributesStr = attributesQuery.ToSQLString()
      query = query
      arguments = arguments
    }
