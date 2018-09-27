module FunWithFlags.FunDB.FunQL.Compiler

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL

type CompiledArgument =
    { placeholder : int
      fieldType : ArgumentFieldType
      valueType : ValueType
    }

type CompiledViewExpr =
    { // Evaluation of column-wise or global attributes
      attributesStr : string
      query : SelectExpr
      arguments : Map<FunQLName, CompiledArgument>
    } with
        override this.ToString () = sprintf "%s; %s" attributesStr (query.ToString())

let private compileScalarType : ScalarType -> AST.SimpleType = function
    | SFTInt -> AST.STInt
    | SFTString -> AST.STString
    | SFTBool -> AST.STBool
    | SFTDateTime -> AST.STDateTime
    | SFTDate -> AST.STDate

let private compileFieldExprType : FieldExprType -> AST.ValueType = function
    | FETScalar stype -> AST.VTScalar <| (compileScalarType stype).ToSQLName()
    | FETArray stype -> AST.VTArray <| (compileScalarType stype).ToSQLName()

let compileFieldType : FieldType -> AST.ValueType = function
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

let private checkPureExpr : ValueExpr<_> -> bool =
    let mutable noReferences = true
    let foundReference column =
        noReferences <- false
    foreachValueExpr foundReference ignore
    noReferences

let private compileName : FunQLName -> AST.SQLName = function
    | FunQLName name -> AST.SQLName name

let compileEntityRef (entityRef : EntityRef) : AST.TableRef = { AST.schema = Option.map compileName entityRef.schema; AST.name = compileName entityRef.name }

let private compileFieldName : ResolvedFieldRef -> ResolvedColumnRef = function
    | RFField (entityRef, fieldName) -> { AST.table = compileEntityRef entityRef; AST.name = compileName fieldName }
    | RFEntityId entityRef -> { AST.table = compileEntityRef entityRef; AST.name = compileName funId }
    | RFSubquery (entityName, fieldName) -> { AST.table = { AST.schema = None; AST.name = compileName entityName }; AST.name = compileName fieldName }

let compileArray (vals : 'a array) : AST.ValueArray<'a> = Array.map AST.AVValue vals

let private compileFieldValue : FieldValue -> AST.ValueExpr = function
    | FInt i -> AST.VEValue (AST.VInt i)
    // PostgreSQL cannot deduce text's type on its own
    | FString s -> AST.VECast (AST.VTScalar AST.STString, AST.VEValue (AST.VString s))
    | FBool b -> AST.VEValue (AST.VBool b)
    | FDateTime dt -> AST.VEValue (AST.VDateTime dt)
    | FDate d -> AST.VEValue (AST.VDate d)
    | FIntArray vals -> AST.VEValue (AST.VIntArray (compileArray vals))
    | FStringArray vals -> AST.VEValue (AST.VStringArray (compileArray vals))
    | FBoolArray vals -> AST.VEValue (AST.VBoolArray (compileArray vals))
    | FDateTimeArray vals -> AST.VEValue (AST.VDateTimeArray (compileArray vals))
    | FDateArray vals -> AST.VEValue (AST.VDateArray (compileArray vals))
    | FNull -> AST.VEValue AST.VNull

let genericCompileFieldExpr (columnFunc : ResolvedFieldRef -> 'f) (placeholderFunc : string -> int) (expr : ParsedFieldExpr) : FieldExpr<'f> =
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
        | FEIsNull a -> AST.VEIsNull (traverse a)
        | FEIsNotNull a -> AST.VEIsNotNull (traverse a)
    traverse

type private QueryCompiler (layout : Layout, placeholders : Map<FunQLName, int>) =
    let compileFieldExpr = genericCompileFieldExpr compileFieldName (fun name -> placeholders.[name])
    let compileLocalFieldExpr tableRef =
        let makeFullName name = { AST.table = tableRef; AST.name = AST.SQLName (name.ToString()) }
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
        genericCompileFieldExpr makeFullName voidPlaceholder

    let compileAttribute prefix (FunQLName name) expr = AST.SCExpr (AST.SQLName <| sprintf "__%s__%s" prefix name) (compileFieldExpr expr)

    let compileResult result = AST.SCExpr (compileName result.name, compileFieldExpr result.expr)

    let rec compileFrom = function
        | FEntity entityRef ->
            let Some entity = layout.FindEntity entityRef
            let tableRef = compileEntityRef entityRef
            let idColumn = AST.SCColumn { AST.table = tableRef; AST.name = SQLName "Id" }
            { tableRef = tableRef
              columns = Seq.singleton idColumn
            }

            let columnFields = entity.columnFields |> Map.toSeq |> Seq.map (fun (name, field) -> AST.SCColumn { AST.table = tableRef; AST.name = compileName name })
            let computedFields = entity.computedFields |> Map.toSeq |> Seq.map (fun (name, field) -> AST.SCExpr (compileName name, compileLocalFieldExpr tableRef field.expression))
            let subquery = {
                AST.columns = Seq.append columnFields computedFields |> Seq.toArray
                AST.clause = {
                    AST.from = AST.FTable entity.name
                    AST.where = None
                    AST.orderBy = [||]
                    AST.limit = None
                    AST.offset = None
                }
            }
            AST.FSubExpr (tableRef, subquery)
        | FJoin (jt, e1, e2, where) -> AST.FJoin (compileJoin jt, compileFrom e1, compileFrom e2, compileFieldExpr where)
        | FSubExpr (name, q) -> AST.FSubExpr (name, compileQueryExpr q)

    and compileFromClause clause =
        { AST.from = compileFrom clause.from
          AST.where = Option.map compileFieldExpr query.where
          AST.orderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr expr)) query.orderBy
          // FIXME: support them!
          AST.limit = None
          AST.offset = None
        }

    and compileQueryExpr query =
        { AST.columns = Array.map (fun (attr, res) -> compileResult res) query.results
          AST.clause = Some (compileFrom query.from)
        }

    member this.CompileFromClause = compileFromClause
    member this.CompileAttribute = compileAttribute
    member this.CompileResult = compileResult

let compileViewExpr (layout : Layout) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let arguments = viewExpr.arguments |> Map.toSeq |> Seq.mapi (fun i (name, fieldType) -> (name, { placeholder = i; fieldType = fieldType; valueType = compileFieldType fieldType })) |> Map.ofSeq
    let placeholders = Map.map (fun arg -> arg.placeholder) arguments
    let compiler = QueryCompiler layout placeholders

    let (queryAttributes, rowAttributes) =
        viewExpr.attributes
            |> Map.toArray
            |> Array.map (fun (name, expr) -> (expr, compiler.CompileAttribute "Row" name expr))
            |> Array.partition (fun (expr, col) -> checkPureExpr expr)
    let (columnAttributes, cellAttributes) =
        viewExpr.results
            |> Array.map (fun (attrs, result) -> attrs |> Map.toArray |> Array.map (fun (name, expr) -> (expr, compiler.CompileAttribute (sprintf "Cell__%s" result.name) name expr)))
            |> Array.concat
            |> Array.partition (fun (expr, col) -> checkPureExpr expr)
    let attributesQuery =
        { columns = Array.append (Array.map snd queryAttributes) (Array.map snd columnAttributes)
          clause = None
        }

    let queryClause = compiler.CompileFromClause viewExpr.clause
    let query =
        { columns = Array.concat [ Array.map snd rowAttributes; Array.map snd cellAttributes; Array.map (fun (attr, res) -> compiler.CompileResult res) viewExpr.results ]
          clause = Some queryClause
        }

    let extractFieldType (attr, result) =
        match result.expr with
            | FEColumn (RFField fieldRef) -> Some fieldRef
            | _ -> None
    let columns = Array.map extractFieldType viewExpr.results

    { attributesStr = attributesQuery.ToSQLString()
      query = query
      columns = columns
      arguments = arguments
    }
