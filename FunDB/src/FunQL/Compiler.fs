module FunWithFlags.FunDB.FunQL.Compiler

open System

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Meta
module SQL = FunWithFlags.FunDB.SQL.AST

let compileName : FunQLName -> SQL.SQLName = function
    | FunQLName name -> SQL.SQLName name

let sqlFunId = compileName funId
let sqlFunView = compileName funView

type CompiledArgument =
    { placeholder : int
      fieldType : ParsedFieldType
      valueType : SQL.SimpleValueType
    }

type private ArgumentsMap = Map<Placeholder, CompiledArgument>

let typecheckArgument (fieldType : FieldType<_, _>) (value : FieldValue) : Result<unit, string> =
    match fieldType with
    | FTEnum vals ->
        match value with
        | FString str when Set.contains str vals -> Ok ()
        | _ -> Error <| sprintf "Argument is not from allowed values of a enum: %O" value
    // Most casting/typechecking will be done by database or Npgsql
    | _ -> Ok ()

let defaultCompiledExprArgument : FieldExprType -> FieldValue = function
    | FETArray SFTString -> FStringArray [||]
    | FETArray SFTInt -> FStringArray [||]
    | FETArray SFTDecimal -> FStringArray [||]
    | FETArray SFTBool -> FStringArray [||]
    | FETArray SFTDateTime -> FStringArray [||]
    | FETArray SFTDate -> FStringArray [||]
    | FETScalar SFTString -> FString ""
    | FETScalar SFTInt -> FInt 0
    | FETScalar SFTDecimal -> FDecimal 0m
    | FETScalar SFTBool -> FBool false
    | FETScalar SFTDateTime -> FDateTime DateTimeOffset.UnixEpoch
    | FETScalar SFTDate -> FDateTime DateTimeOffset.UnixEpoch

let defaultCompiledArgument : ParsedFieldType -> FieldValue = function
    | FTType feType -> defaultCompiledExprArgument feType
    | FTReference (entityRef, None) -> FInt 0
    | FTReference (entityRef, Some where) -> failwith "Reference with a condition in an argument"
    | FTEnum values -> values |> Set.toSeq |> Seq.first |> Option.get |> FString

// Evaluation of column-wise or global attributes
type CompiledAttributesExpr =
    { query : string
      pureAttributes : Set<AttributeName>
      pureColumnAttributes : Map<FieldName, Set<AttributeName>>
    }

type CompiledViewExpr =
    { attributesExpr : CompiledAttributesExpr option
      query : SQL.SelectExpr
      arguments : ArgumentsMap
    }

let private compileScalarType : ScalarFieldType -> SQL.SimpleType = function
    | SFTInt -> SQL.STInt
    | SFTDecimal -> SQL.STDecimal
    | SFTString -> SQL.STString
    | SFTBool -> SQL.STBool
    | SFTDateTime -> SQL.STDateTime
    | SFTDate -> SQL.STDate

let private compileFieldExprType : FieldExprType -> SQL.SimpleValueType = function
    | FETScalar stype -> SQL.VTScalar <| compileScalarType stype
    | FETArray stype -> SQL.VTArray <| compileScalarType stype

let compileFieldType : FieldType<_, _> -> SQL.SimpleValueType = function
    | FTType fetype -> compileFieldExprType fetype
    | FTReference (ent, restriction) -> SQL.VTScalar SQL.STInt
    | FTEnum vals -> SQL.VTScalar SQL.STString

let private compileOrder : SortOrder -> SQL.SortOrder = function
    | Asc -> SQL.Asc
    | Desc -> SQL.Desc

let private compileJoin : JoinType -> SQL.JoinType = function
    | Left -> SQL.Left
    | Right -> SQL.Right
    | Inner -> SQL.Inner
    | Outer -> SQL.Full

let private checkPureExpr (expr : FieldExpr<_>) : bool =
    let mutable noReferences = true
    let foundReference column =
        noReferences <- false
    iterFieldExpr foundReference ignore expr
    noReferences

let private checkArgumentPureExpr (expr : FieldExpr<_>) : bool =
    let mutable noReferences = true
    let foundReference column =
        noReferences <- false
    let foundPlaceholder placeholder =
        noReferences <- false
    iterFieldExpr foundReference foundPlaceholder expr
    noReferences

let private compileEntityPun (entityRef : ResolvedEntityRef) : SQL.TableName = SQL.SQLName <| sprintf "%O__%O" entityRef.schema entityRef.name

let private compileEntityTablePun (entityRef : ResolvedEntityRef) : SQL.TableRef =
    { schema = None; name = compileEntityPun entityRef }

let compileEntityRef (entityRef : EntityRef) : SQL.TableRef = { schema = Option.map compileName entityRef.schema; name = compileName entityRef.name }

let compileResolvedEntityRef (entityRef : ResolvedEntityRef) : SQL.TableRef = { schema = Some (compileName entityRef.schema); name = compileName entityRef.name }

let compileFieldRef (fieldRef : ResolvedFieldRef) : SQL.ColumnRef =
    { table = Some <| compileResolvedEntityRef fieldRef.entity; name = compileName fieldRef.name }

let private compileFieldName : ResolvedFieldName -> SQL.ColumnRef = function
    | RFField fieldRef -> { table = Some <| compileEntityTablePun fieldRef.entity; name = compileName fieldRef.name }
    | RFSubquery (entityName, fieldName, boundField) -> { table = Some { schema = None; name = compileName entityName }; name = compileName fieldName }

let compileArray (vals : 'a array) : SQL.ValueArray<'a> = Array.map SQL.AVValue vals

let compileFieldValue : FieldValue -> SQL.ValueExpr = function
    | FInt i -> SQL.VEValue (SQL.VInt i)
    | FDecimal d -> SQL.VEValue (SQL.VDecimal d)
    // PostgreSQL cannot deduce text's type on its own
    | FString s -> SQL.VECast (SQL.VEValue (SQL.VString s), SQL.VTScalar (SQL.STString.ToSQLName()))
    | FBool b -> SQL.VEValue (SQL.VBool b)
    | FDateTime dt -> SQL.VEValue (SQL.VDateTime dt)
    | FDate d -> SQL.VEValue (SQL.VDate d)
    | FIntArray vals -> SQL.VEValue (SQL.VIntArray (compileArray vals))
    | FDecimalArray vals -> SQL.VEValue (SQL.VDecimalArray (compileArray vals))
    | FStringArray vals -> SQL.VEValue (SQL.VStringArray (compileArray vals))
    | FBoolArray vals -> SQL.VEValue (SQL.VBoolArray (compileArray vals))
    | FDateTimeArray vals -> SQL.VEValue (SQL.VDateTimeArray (compileArray vals))
    | FDateArray vals -> SQL.VEValue (SQL.VDateArray (compileArray vals))
    | FNull -> SQL.VEValue SQL.VNull

let genericCompileFieldExpr (columnFunc : 'c -> SQL.ColumnRef) (placeholderFunc : Placeholder -> int) : FieldExpr<'c> -> SQL.ValueExpr =
    let rec traverse = function
        | FEValue v -> compileFieldValue v
        | FEColumn c -> SQL.VEColumn (columnFunc c)
        | FEPlaceholder name -> SQL.VEPlaceholder (placeholderFunc name)
        | FENot a -> SQL.VENot (traverse a)
        | FEAnd (a, b) -> SQL.VEAnd (traverse a, traverse b)
        | FEOr (a, b) -> SQL.VEOr (traverse a, traverse b)
        | FEConcat (a, b) -> SQL.VEConcat (traverse a, traverse b)
        | FEEq (a, b) -> SQL.VEEq (traverse a, traverse b)
        | FENotEq (a, b) -> SQL.VENotEq (traverse a, traverse b)
        | FELike (e, pat) -> SQL.VELike (traverse e, traverse pat)
        | FENotLike (e, pat) -> SQL.VENotLike (traverse e, traverse pat)
        | FELess (a, b) -> SQL.VELess (traverse a, traverse b)
        | FELessEq (a, b) -> SQL.VELessEq (traverse a, traverse b)
        | FEGreater (a, b) -> SQL.VEGreater (traverse a, traverse b)
        | FEGreaterEq (a, b) -> SQL.VEGreaterEq (traverse a, traverse b)
        | FEIn (a, arr) -> SQL.VEIn (traverse a, Array.map traverse arr)
        | FENotIn (a, arr) -> SQL.VENotIn (traverse a, Array.map traverse arr)
        | FECast (e, typ) -> SQL.VECast (traverse e, SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLName()) (compileFieldExprType typ))
        | FEIsNull a -> SQL.VEIsNull (traverse a)
        | FEIsNotNull a -> SQL.VEIsNotNull (traverse a)
        | FECase (es, els) -> SQL.VECase (Array.map (fun (cond, expr) -> (traverse cond, traverse expr)) es, Option.map traverse els)
        | FECoalesce arr -> SQL.VECoalesce (Array.map traverse arr)
    traverse

// Differs from compileFieldValue in that it doesn't emit value expressions.
let compileArgument : FieldValue -> SQL.Value = function
    | FInt i -> SQL.VInt i
    | FDecimal d -> SQL.VDecimal d
    | FString s -> SQL.VString s
    | FBool b -> SQL.VBool b
    | FDateTime dt -> SQL.VDateTime dt
    | FDate d -> SQL.VDate d
    | FIntArray vals -> SQL.VIntArray (compileArray vals)
    | FDecimalArray vals -> SQL.VDecimalArray (compileArray vals)
    | FStringArray vals -> SQL.VStringArray (compileArray vals)
    | FBoolArray vals -> SQL.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> SQL.VDateTimeArray (compileArray vals)
    | FDateArray vals -> SQL.VDateArray (compileArray vals)
    | FNull -> SQL.VNull

let private compileConditionClauseGeneric (arguments : ArgumentsMap) (clause : ResolvedConditionClause) : SQL.ConditionClause =
    let compileFieldExpr = genericCompileFieldExpr compileFieldName (fun name -> arguments.[name].placeholder)
    { where = Option.map compileFieldExpr clause.where
      orderBy = Array.map (fun (ord, expr) -> (compileOrder ord, compileFieldExpr expr)) clause.orderBy
      // FIXME: support them!
      limit = None
      offset = None
    }

type private QueryCompiler (layout : Layout, arguments : ArgumentsMap) =
    let compileFieldExpr = genericCompileFieldExpr compileFieldName (fun name -> arguments.[name].placeholder)
    let compileLocalFieldExpr (tableRef : SQL.TableRef option) : LocalFieldExpr -> SQL.ValueExpr =
        let makeFullName name = { table = tableRef; name = compileName name } : SQL.ColumnRef
        let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
        genericCompileFieldExpr makeFullName voidPlaceholder

    let compileAttribute prefix (FunQLName name) expr = SQL.SCExpr (SQL.SQLName <| sprintf "__%s__%s" prefix name, compileFieldExpr expr)

    let compileResult (result : ResolvedQueryResult) =
        match result.expression with
        | FEColumn c -> SQL.SCColumn (compileFieldName c)
        | _ -> SQL.SCExpr (compileName result.name, compileFieldExpr result.expression)

    let findReference (name : FieldName) (field : ResolvedColumnField) : ResolvedFieldRef option =
        match field.fieldType with
        | FTReference (entityRef, where) ->
            let entity = Option.get <| layout.FindEntity(entityRef)
            Some { entity = entityRef; name = entity.mainField }
        | _ -> None

    let rec compileFrom : ResolvedFromExpr -> SQL.FromExpr = function
        | FEntity entityRef ->
            let entity = Option.get <| layout.FindEntity(entityRef)
            let tableRef = compileResolvedEntityRef entityRef
            let idColumn = SQL.SCColumn { table = Some tableRef; name = sqlFunId }

            let columnFields = entity.columnFields |> Map.toSeq |> Seq.map (fun (name, field) -> SQL.SCColumn { table = Some tableRef; name = compileName name })
            let computedFields = entity.computedFields |> Map.toSeq |> Seq.map (fun (name, field) -> SQL.SCExpr (compileName name, compileLocalFieldExpr (Some tableRef) field.expression))
            let references = Map.update findReference entity.columnFields

            let buildReferences (from : SQL.FromExpr) (fieldName : FieldName) (referenceRef : ResolvedFieldRef) =
                let columnName = compileName fieldName
                let table = compileResolvedEntityRef referenceRef.entity
                let where = SQL.VEEq (SQL.VEColumn { table = Some tableRef; name = columnName }, SQL.VEColumn { table = Some table; name = sqlFunId }) : SQL.ValueExpr
                SQL.FJoin (SQL.Left, from, SQL.FTable table, where)
            let from = Map.fold buildReferences (SQL.FTable tableRef) references

            let makeReferenceName (fieldName : FieldName, referenceRef : ResolvedFieldRef) =
                SQL.SCExpr (SQL.SQLName <| sprintf "__Name__%O" fieldName, SQL.VEColumn <| compileFieldRef referenceRef)
            let referenceNames = references |> Map.toSeq |> Seq.map makeReferenceName

            let subquery =
                { columns = Seq.append (Seq.append (Seq.singleton idColumn) (Seq.append columnFields computedFields)) referenceNames |> Seq.toArray 
                  clause =
                      Some { from = from
                             condition = { where = None
                                           orderBy = [||]
                                           limit = None
                                           offset = None
                                         }
                           }
                } : SQL.SelectExpr
            
            SQL.FSubExpr (compileEntityPun entityRef, subquery)
        | FJoin (jt, e1, e2, where) -> SQL.FJoin (compileJoin jt, compileFrom e1, compileFrom e2, compileFieldExpr where)
        // FIXME: track field names across subexpressions
        | FSubExpr (name, q) -> SQL.FSubExpr (compileName name, compileQueryExpr q)

    and compileConditionClause = compileConditionClauseGeneric arguments

    and compileFromClause (clause : ResolvedFromClause) : SQL.FromClause =
        { from = compileFrom clause.from
          condition = compileConditionClause clause.condition
        }

    and compileQueryExpr (query : ResolvedQueryExpr) : SQL.SelectExpr =
        { columns = Array.map compileResult query.results
          clause = Some <| compileFromClause query.clause
        }

    member this.CompileFromClause = compileFromClause
    member this.CompileAttribute = compileAttribute
    member this.CompileResult = compileResult

let compileViewExpr (layout : Layout) (viewExpr : ResolvedViewExpr) : CompiledViewExpr =
    let convertArgument i (name, fieldType) =
        let info = {
            placeholder = i
            fieldType = fieldType
            valueType = compileFieldType fieldType
        }
        (name, info)

    let arguments =
        viewExpr.arguments
            |> Map.toSeq
            |> Seq.mapi convertArgument
            |> Map.ofSeq

    let compiler = QueryCompiler (layout, arguments)

    let mapColumnAttribute (columnName : FieldName) (name : AttributeName) (expr : ResolvedFieldExpr) =
        (expr, compiler.CompileAttribute (sprintf "Cell__%O" columnName) name expr)

    let (queryAttributes, rowAttributes) =
        viewExpr.attributes
            |> Map.map (fun name expr -> (expr, compiler.CompileAttribute "Row" name expr))
            |> Map.partition (fun name (expr, col) -> checkPureExpr expr)
    let columnCellAttributes =
        viewExpr.results
            |> Seq.map (fun (attrs, result) -> (result.name, attrs |> Map.map (mapColumnAttribute result.name) |> Map.partition (fun name (expr, col) -> checkPureExpr expr)))
            |> Map.ofSeq
    let columnAttributes = Map.map (fun name -> fst) columnCellAttributes
    let cellAttributes = Map.map (fun name -> snd) columnCellAttributes

    let toResults = Map.toSeq >> Seq.map (fun (name, (expr, col)) -> col)
    let columnToResults = Map.toSeq >> Seq.map (fun (name, attrs) -> toResults attrs) >> Seq.concat

    let attributeColumns = Seq.append (toResults queryAttributes) (columnToResults columnAttributes) |> Array.ofSeq
    let attributesExpr =
        if Array.isEmpty attributeColumns
        then None
        else
            let pureAttributes = Map.filter (fun name (expr, col) -> checkArgumentPureExpr expr) queryAttributes
            let pureColumnAttributes = Map.map (fun name args -> Map.filter (fun name (expr, col) -> checkArgumentPureExpr expr) args) columnAttributes

            let attributesQuery : SQL.SelectExpr =
                { columns = attributeColumns
                  clause = None
                }
            let attributesExpr =
                { query = attributesQuery.ToSQLString()
                  pureAttributes = Map.keysSet pureAttributes
                  pureColumnAttributes = Map.map (fun name -> Map.keysSet) pureColumnAttributes
                }
            Some attributesExpr

    let resultColumns = Seq.map (fun (attr, res) -> compiler.CompileResult res) viewExpr.results
    let getNameAttribute (attributes, result : ResolvedQueryResult) =
        match result.expression with
        | FEColumn (RFField fieldRef) when fieldRef.name <> funId ->
            let field = Option.get <| layout.FindField fieldRef.entity fieldRef.name
            match field with
            | RColumnField { fieldType = FTReference (entityRef, where) } ->
                let nameColumn = { table = Some <| compileEntityTablePun fieldRef.entity; name = SQL.SQLName <| sprintf "__Name__%O" fieldRef.name } : SQL.ColumnRef
                Some <| SQL.SCExpr (SQL.SQLName <| sprintf "__Name__%O" result.name, SQL.VEColumn nameColumn)
            | _ -> None
        | _ -> None
    let nameAttributes =
        viewExpr.results
        |> Seq.mapMaybe getNameAttribute

    let idColumn =
        match viewExpr.update with
        | None -> Seq.empty
        | Some updateExpr ->
            Seq.singleton <| SQL.SCExpr (SQL.SQLName "__Id", SQL.VEColumn { table = Some <| compileEntityTablePun updateExpr.entity; name = sqlFunId })

    let queryClause = compiler.CompileFromClause viewExpr.clause
    let query =
        { columns = [ idColumn; toResults rowAttributes; columnToResults cellAttributes; resultColumns; nameAttributes ] |> Seq.concat |> Array.ofSeq
          clause = Some queryClause
        } : SQL.SelectExpr

    { attributesExpr = attributesExpr
      query = query
      arguments = arguments
    }

let compileAddedCondition (viewExpr : CompiledViewExpr) (condClause : ResolvedConditionClause) : CompiledViewExpr =
    let newClause =
        { from = SQL.FSubExpr (sqlFunView, viewExpr.query)
          condition = compileConditionClauseGeneric viewExpr.arguments condClause
        } : SQL.FromClause
    let newQuery =
        { columns = [| SQL.SCAll |]
          clause = Some newClause
        } : SQL.SelectExpr
    { viewExpr with
          query = newQuery
    }