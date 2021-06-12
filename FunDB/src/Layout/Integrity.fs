module FunWithFlags.FunDB.Layout.Integrity

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open System.Text
open System.Data.HashFunction.CityHash

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
module PLPgSQL = FunWithFlags.FunDB.SQL.PLPgSQL

type LayoutIntegrityException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = LayoutIntegrityException (message, null)

type ColumnOfTypeAssertion =
    { FromField : ResolvedFieldRef
      ToEntity : ResolvedEntityRef
    }

type LayoutAssertions =
    { ColumnOfTypeAssertions : Set<ColumnOfTypeAssertion>
      CheckConstraints : Map<ResolvedConstraintRef, ResolvedFieldExpr>
    }

let unionLayoutAssertions (a : LayoutAssertions) (b : LayoutAssertions) : LayoutAssertions =
    { ColumnOfTypeAssertions = Set.union a.ColumnOfTypeAssertions b.ColumnOfTypeAssertions
      CheckConstraints = Map.union a.CheckConstraints b.CheckConstraints
    }

let differenceLayoutAssertions (a : LayoutAssertions) (b : LayoutAssertions) : LayoutAssertions =
    { ColumnOfTypeAssertions = Set.difference a.ColumnOfTypeAssertions b.ColumnOfTypeAssertions
      CheckConstraints = Map.difference a.CheckConstraints b.CheckConstraints
    }

let emptyLayoutAssertions : LayoutAssertions =
    { ColumnOfTypeAssertions = Set.empty
      CheckConstraints = Map.empty
    }

type private AssertionsBuilder (layout : Layout) =
    let columnFieldAssertions (fieldRef : ResolvedFieldRef) (field : ResolvedColumnField) : LayoutAssertions seq =
        seq {
            match field.FieldType with
            | FTReference toRef ->
                let refEntity = layout.FindEntity toRef |> Option.get
                if Option.isSome refEntity.Parent then
                    let assertion =
                        { FromField = fieldRef
                          ToEntity = toRef
                        }
                    yield
                        { emptyLayoutAssertions with
                              ColumnOfTypeAssertions = Set.singleton assertion
                        }
            | _ -> ()
        }

    let entityAssertions (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : LayoutAssertions seq =
        seq {
            for KeyValue (colName, col) in entity.ColumnFields do
                let ref = { Entity = entityRef; Name = colName }
                yield! columnFieldAssertions ref col
            for KeyValue (constrName, constr) in entity.CheckConstraints do
                if not constr.IsLocal then
                    let ref = { Entity = entityRef; Name = constrName }
                    yield
                        { emptyLayoutAssertions with
                              CheckConstraints = Map.singleton ref constr.Expression
                        }
        }

    let schemaAssertions (schemaName : SchemaName) (schema : ResolvedSchema) =
        seq {
            for KeyValue (entityName, entity) in schema.Entities do
                let ref = { Schema = schemaName; Name = entityName }
                yield! entityAssertions ref entity
        }

    let layoutAssertions (subLayout : Layout) =
        seq {
            for KeyValue (schemaName, schema) in subLayout.Schemas do
                yield! schemaAssertions schemaName schema
        }

    member this.BuildAssertions subLayout = layoutAssertions subLayout

let buildAssertions (layout : Layout) (subLayout : Layout) : LayoutAssertions =
    let builder = AssertionsBuilder layout
    builder.BuildAssertions subLayout |> Seq.fold unionLayoutAssertions emptyLayoutAssertions

let private sqlOldName = SQL.SQLName "old"
let private sqlNewName = SQL.SQLName "new"

let private sqlOldRow : SQL.TableRef = { Schema = None; Name = sqlOldName }
let private sqlNewRow : SQL.TableRef = { Schema = None; Name = sqlNewName }

let private sqlNewId : SQL.ColumnRef = { Table = Some sqlNewRow; Name = sqlFunId }
let private sqlPlainId : SQL.ColumnRef = { Table = None; Name = sqlFunId }

let private returnNullStatement = PLPgSQL.StReturn (SQL.VEValue SQL.VNull)

let private plainSubEntityColumn = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

type private CheckTriggerOptions =
    { FunctionName : SQL.SQLName
      UpdateTriggerName : SQL.SQLName
      InsertTriggerName : SQL.SQLName
      Entity : ResolvedEntityRef
      AffectedFields : Set<FieldName>
      ExtraUpdateFieldCondition : ResolvedColumnField -> SQL.ValueExpr -> SQL.ValueExpr -> SQL.ValueExpr
      Expression : SQL.SelectExpr
    }

let buildColumnOfTypeAssertion (layout : Layout) (fromFieldRef : ResolvedFieldRef) (toEntityRef : ResolvedEntityRef) : SQL.DatabaseMeta =
    let entity = layout.FindEntity fromFieldRef.Entity |> Option.get
    let field = Map.find fromFieldRef.Name entity.ColumnFields
    let refEntity = layout.FindEntity toEntityRef |> Option.get
    let checkExpr = makeCheckExpr plainSubEntityColumn layout toEntityRef

    let toRef = compileResolvedEntityRef refEntity.Root
    let fromSchema = compileName entity.Root.Schema
    let fromTable = compileName entity.Root.Name
    let fromColumn = SQL.VEColumn { Table = Some sqlNewRow; Name = field.ColumnName }
    let toColumn = SQL.VEColumn { Table = Some toRef; Name = sqlFunId }
    let whereExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
    let singleSelect =
        { Columns = [| SQL.SCExpr (None, checkExpr) |]
          From = Some <| SQL.FTable (null, None, toRef)
          Where = Some whereExpr
          GroupBy = [||]
          OrderLimit = SQL.emptyOrderLimitClause
          Extra = null
        } : SQL.SingleSelectExpr
    let raiseCall =
        { Level = PLPgSQL.RLException
          Message = None
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VEValue (SQL.VString "integrity_constraint_violation"))
                  (PLPgSQL.ROColumn, SQL.VEValue (SQL.VString <| fromFieldRef.Name.ToString()))
                  (PLPgSQL.ROTable, SQL.VEValue (SQL.VString <| fromFieldRef.Entity.Name.ToString()))
                  (PLPgSQL.ROSchema, SQL.VEValue (SQL.VString <| fromFieldRef.Entity.Schema.ToString()))
                ]
        } : PLPgSQL.RaiseStatement
    let selectExpr : SQL.SelectExpr =
        { CTEs = None
          Tree = SQL.SSelect singleSelect
          Extra = null
        }
    let checkStmt = PLPgSQL.StIfThenElse ([| (SQL.VENot (SQL.VESubquery selectExpr), [| PLPgSQL.StRaise raiseCall |]) |], None)

    let checkProgram =
        { Declarations = [||]
          Body = [| checkStmt; returnNullStatement |]
        } : PLPgSQL.Program
    let checkFunctionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue (SQL.SQLRawString "trigger")
          Behaviour = SQL.FBStable
          Language = PLPgSQL.plPgSQLName
          Definition = checkProgram.ToPLPgSQLString()
        } : SQL.FunctionDefinition
    let checkFunctionKey = sprintf "__ref_type_check__%s__%s" entity.HashName field.HashName
    let checkFunctionName = SQL.SQLName checkFunctionKey
    let checkFunctionObject = SQL.OMFunction <| Map.singleton [||] checkFunctionDefinition

    let checkOldColumn = SQL.VEColumn { Table = Some sqlOldRow; Name = field.ColumnName }
    let checkNewColumn = SQL.VEColumn { Table = Some sqlNewRow; Name = field.ColumnName }
    let checkUpdateTriggerCondition =
        if field.IsNullable then
            SQL.VEAnd (SQL.VEIsNotNull (checkNewColumn), SQL.VEDistinct (checkOldColumn, checkNewColumn))
        else
            SQL.VEBinaryOp (checkOldColumn, SQL.BONotEq, checkNewColumn)
    let checkUpdateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate (Some [| field.ColumnName |]) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable checkUpdateTriggerCondition
          FunctionName = { Schema = Some fromSchema; Name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkUpdateTriggerKey = sprintf "__ref_type_update__%s__%s" entity.HashName field.HashName
    let checkUpdateTriggerName = SQL.SQLName checkUpdateTriggerKey
    let checkUpdateTriggerObject = SQL.OMTrigger (fromTable, checkUpdateTriggerDefinition)

    let checkInsertTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEInsert |]
          Mode = SQL.TMEachRow
          Condition = None
          FunctionName = { Schema = Some fromSchema; Name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkInsertTriggerKey = sprintf "__ref_type_insert__%s__%s" entity.HashName field.HashName
    let checkInsertTriggerName = SQL.SQLName checkInsertTriggerKey
    let checkInsertTriggerObject = SQL.OMTrigger (fromTable, checkInsertTriggerDefinition)

    let objects =
        [ (checkFunctionKey, (checkFunctionName, checkFunctionObject))
          (checkUpdateTriggerKey, (checkUpdateTriggerName, checkUpdateTriggerObject))
          (checkInsertTriggerKey, (checkInsertTriggerName, checkInsertTriggerObject))
        ]
        |> Map.ofSeq
    { Schemas = Map.singleton (fromSchema.ToString()) { Name = fromSchema; Objects = objects }
      Extensions = Set.empty
    }

// Need to be public for JsonConvert
type PathTriggerKey =
    { Ref : ResolvedFieldRef
      // This is path without the last dereference; for example, triggers for foo=>bar and foo=>baz will be merged into trigger for entity E, which handles bar and baz.
      EntityPath : FieldName list
    }

let private pathTriggerHasher =
    let config = CityHashConfig()
    config.HashSizeInBits <- 128
    CityHashFactory.Instance.Create(config)

type PathTriggerFullKey =
    { ConstraintRef : ResolvedFieldRef
      Key : PathTriggerKey
    }

// As we can't compress PathTriggerKey into 63 symbols (PostgreSQL restriction!) we hash it instead.
let private pathTriggerName (key : PathTriggerFullKey) =
    let pathStr = JsonConvert.SerializeObject key
    let pathBytes = Encoding.UTF8.GetBytes pathStr
    let str = pathTriggerHasher.ComputeHash(pathBytes).AsHexString()
    String.truncate 40 str

type private PathTrigger =
    { // Map from field names to field root entities.
      Fields : Map<FieldName, ResolvedEntityRef>
      Root : ResolvedEntityRef
      Expression : SQL.ValueExpr
    }

let private unionPathTrigger (a : PathTrigger) (b : PathTrigger) : PathTrigger =
    { // Values should be the same.
      Fields = Map.union a.Fields b.Fields
      Root = b.Root
      Expression = b.Expression
    }

let private expandPathTriggerExpr (outerField : FieldName) (innerEntity : ResolvedEntityRef) (innerExpr : SQL.ValueExpr) : SQL.ValueExpr =
    let singleSelectExpr : SQL.SingleSelectExpr =
        { Columns = [| SQL.SCExpr (None, SQL.VEColumn sqlPlainId) |]
          From = Some (SQL.FTable (null, None, compileResolvedEntityRef innerEntity))
          Extra = null
          Where = Some innerExpr
          GroupBy = [||]
          OrderLimit = SQL.emptyOrderLimitClause
        }
    let selectExpr : SQL.SelectExpr =
        { CTEs = None
          Tree = SQL.SSelect singleSelectExpr
          Extra = null
        }
    let leftRef : SQL.ColumnRef = { Table = None; Name = compileName outerField }
    SQL.VEInQuery (SQL.VEColumn leftRef, selectExpr)

let private postprocessTrigger (compiledRef : SQL.TableRef) (trig : PathTrigger) : PathTrigger =
    // Replace outer table-less references with named references, so that we can insert this expression
    // into compiled FunQL query.
    // For example:
    // > foo IN (SELECT id FROM ent WHERE bar = new.id)
    // Becomes:
    // > schema__other_ent.foo IN (SELECT id FROM ent WHERE bar = new.id)
    // Also:
    // > foo <> new.id
    // Becomes:
    // > schema__other_ent.foo <> new.id
    let mapRef (ref : SQL.ColumnRef) =
        match ref.Table with
        | None -> { ref with Table = Some compiledRef }
        | Some _ -> ref
    let mapper = { SQL.idValueExprMapper with ColumnReference = mapRef }
    let newExpr = SQL.mapValueExpr mapper trig.Expression
    { trig with Expression = newExpr }

type private CheckConstraintAffectedBuilder (layout : Layout, constrEntityRef : ResolvedEntityRef) =
    let rec buildSinglePathTriggers (extra : ObjectMap) (outerRef : ResolvedFieldRef) (refEntityRef : ResolvedEntityRef) (fieldName : FieldName) : (PathTriggerKey * PathTrigger) seq =
        let refEntity = layout.FindEntity refEntityRef |> Option.get
        let refField = refEntity.FindField fieldName |> Option.get

        match refField.Field with
        | RComputedField comp ->
            match comp.VirtualCases with
            | None -> buildExprTriggers comp.Expression
            | Some cases -> computedFieldCases layout extra refField.Name cases |> Seq.map snd |> Seq.collect buildExprTriggers
        | RColumnField col ->
            let leftRef : SQL.ColumnRef = { Table = None; Name = compileName outerRef.Name }
            let expr = SQL.VEBinaryOp (SQL.VEColumn leftRef, SQL.BOEq, SQL.VEColumn sqlNewId)

            let key =
                { Ref = outerRef
                  EntityPath = []
                }
            let trigger =
                { Fields = Map.singleton refField.Name refEntityRef
                  Root = refEntity.Root
                  Expression = expr
                }
            Seq.singleton (key, trigger)
        | _ -> Seq.empty

    and buildPathTriggers (extra : ObjectMap) (outerRef : ResolvedFieldRef) : (ResolvedEntityRef * FieldName) list -> (PathTriggerKey * PathTrigger) seq = function
        | [] -> Seq.empty
        | [(entityRef, fieldName)] -> buildSinglePathTriggers extra outerRef entityRef fieldName
        | (entityRef, fieldName) :: refs ->
            let outerTriggers = buildSinglePathTriggers extra outerRef entityRef fieldName
            let refEntity = layout.FindEntity entityRef |> Option.get
            let refField = refEntity.FindField fieldName |> Option.get

            let innerFieldRef = { Entity = entityRef; Name = refField.Name }
            let innerTriggers = buildPathTriggers extra innerFieldRef refs

            let expandPathTrigger (key, trigger : PathTrigger) =
                let newKey =
                    { Ref = outerRef
                      EntityPath = refField.Name :: key.EntityPath
                    }
                let newTrigger =
                    { trigger with
                          Expression = expandPathTriggerExpr outerRef.Name entityRef trigger.Expression
                    }
                (newKey, newTrigger)
            let wrappedTriggers = Seq.map expandPathTrigger innerTriggers
            Seq.append outerTriggers wrappedTriggers

    and buildExprTriggers (expr : ResolvedFieldExpr) : (PathTriggerKey * PathTrigger) seq =
        let mutable triggers = Seq.empty

        let iterReference (ref : LinkedBoundFieldRef) =
            let fieldInfo = ObjectMap.findType<FieldMeta> ref.Extra
            let boundInfo = Option.get fieldInfo.Bound
            let pathWithEntities = Seq.zip boundInfo.Path ref.Ref.Path |> List.ofSeq
            let newTriggers = buildPathTriggers ref.Extra boundInfo.Ref pathWithEntities
            triggers <- Seq.append triggers newTriggers
        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference
            }
        iterFieldExpr mapper expr

        triggers

    let rec findOuterFields (expr : ResolvedFieldExpr) : Set<FieldName> =
        let allTriggers = buildExprTriggers

        let mutable triggers = Map.empty
        let mutable outerFields = Set.empty

        let iterReference (ref : LinkedBoundFieldRef) =
            let fieldInfo = ObjectMap.findType<FieldMeta> ref.Extra
            let boundInfo = Option.get fieldInfo.Bound
            let field = layout.FindField boundInfo.Ref.Entity boundInfo.Ref.Name |> Option.get
            match field.Field with
            | RComputedField comp ->
                let runForExpr (newExpr : ResolvedFieldExpr) =
                    let newOuterFields = findOuterFields newExpr
                    outerFields <- Set.union outerFields newOuterFields

                match comp.VirtualCases with
                | None -> runForExpr comp.Expression
                | Some cases ->
                    for (case, expr) in computedFieldCases layout ref.Extra boundInfo.Ref.Name cases do
                        runForExpr expr
            | RColumnField col ->
                outerFields <- Set.add field.Name outerFields
            | _ -> ()

        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference
            }
        iterFieldExpr mapper expr

        outerFields

    // Returns encountered outer fields, and also path triggers.
    let findCheckConstraintAffected (expr : ResolvedFieldExpr) : Map<PathTriggerKey, PathTrigger> =
        let sqlRef = compileRenamedResolvedEntityRef constrEntityRef
        buildExprTriggers expr |> Seq.fold (fun trigs (key, trig) -> Map.addWith unionPathTrigger key (postprocessTrigger sqlRef trig) trigs) Map.empty

    member this.FindOuterFields expr = findOuterFields expr
    member this.FindCheckConstraintAffected expr = findCheckConstraintAffected expr

let private compileAggregateCheckConstraintCheck (layout : Layout) (constrRef : ResolvedConstraintRef) (check : ResolvedFieldExpr) : SQL.SelectExpr =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let fixedCheck = replaceEntityRefInExpr (Some <| relaxEntityRef entity.Root) check
    let aggExpr = FEAggFunc (FunQLName "bool_and", AEAll [| fixedCheck |])

    let result =
        { Attributes = Map.empty
          Result = aggExpr
          Alias = None
        }
    let singleSelect =
        { Attributes = Map.empty
          Results = [| QRExpr result |]
          From = Some (FEntity (None, relaxEntityRef entity.Root))
          Where = None
          GroupBy = [||]
          OrderLimit = emptyOrderLimitClause
          Extra = ObjectMap.empty
        } : ResolvedSingleSelectExpr
    let select = { CTEs = None; Tree = SSelect singleSelect; Extra = ObjectMap.empty }
    let (arguments, ret) = compileSelectExpr layout emptyArguments select
    ret

// Replaces entity references with `new` in simple cases.
type private ConstraintUseNewConverter (constrEntityRef : ResolvedEntityRef) =
    let compiledConstrTableName = renameResolvedEntityRef constrEntityRef

    let rec useNewInSelectTreeExpr : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
        | SQL.SSelect query -> SQL.SSelect <| useNewInSingleSelectExpr query
        | SQL.SSetOp setOp ->
            SQL.SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = useNewInSelectExpr setOp.A
                  B = useNewInSelectExpr setOp.B
                  OrderLimit = useNewInOrderLimitClause setOp.OrderLimit
                }
        | SQL.SValues values -> SQL.SValues values

    and useNewInSelectExpr (select : SQL.SelectExpr) : SQL.SelectExpr =
        if Option.isSome select.CTEs then
            failwith "Impossible"
        { CTEs = None
          Tree = useNewInSelectTreeExpr select.Tree
          Extra = null
        }

    and useNewInSingleSelectExpr (query : SQL.SingleSelectExpr) : SQL.SingleSelectExpr =
        let from = Option.get query.From
        let info = query.Extra :?> SelectFromInfo
        let (extraWhere, maybeName, from) = useNewInFromExpr from
        let droppedName = Option.get maybeName
        let currWhere = Option.map useNewInValueExpr info.WhereWithoutSubentities
        let entitiesMap = Map.remove droppedName info.Entities
        let currWhere = addEntityChecks entitiesMap currWhere
        { Columns = Array.map useNewInSelectedColumn query.Columns
          From = from
          Where = Option.unionWith (curry SQL.VEAnd) currWhere extraWhere
          GroupBy = Array.map useNewInValueExpr query.GroupBy
          OrderLimit = useNewInOrderLimitClause query.OrderLimit
          Extra = query.Extra
        }

    and useNewInOrderColumn (col : SQL.OrderColumn) : SQL.OrderColumn =
        { Expr = useNewInValueExpr col.Expr
          Order = col.Order
          Nulls = col.Nulls
        }

    and useNewInOrderLimitClause (clause : SQL.OrderLimitClause) : SQL.OrderLimitClause =
        { Limit = Option.map useNewInValueExpr clause.Limit
          Offset = Option.map useNewInValueExpr clause.Offset
          OrderBy = Array.map useNewInOrderColumn clause.OrderBy
        }

    and useNewInSelectedColumn : SQL.SelectedColumn -> SQL.SelectedColumn = function
        | SQL.SCAll _ -> failwith "Unexpected SELECT *"
        | SQL.SCExpr (name, expr) -> SQL.SCExpr (name, useNewInValueExpr expr)

    and useNewInValueExpr =
        let renamesMap = Map.singleton compiledConstrTableName sqlNewName
        renameValueExprTables renamesMap

    // ValueExpr returned is a piece that goes into WHERE clause.
    and useNewInFromExpr : SQL.FromExpr -> (SQL.ValueExpr option * SQL.TableName option * SQL.FromExpr option) = function
        | SQL.FTable (extra, pun, entity) as from ->
            let ann = extra :?> RealEntityAnnotation
            if not ann.FromPath && ann.RealEntity = constrEntityRef then
                (None, pun |> Option.map (fun x -> x.Name), None)
            else
                (None, None, Some from)
        | SQL.FJoin join as expr ->
            let (valExprA, nameA, ma) = useNewInFromExpr join.A
            let (valExprB, nameB, mb) = useNewInFromExpr join.B
            let name = Option.unionUnique nameA nameB
            let valExpr = Option.unionWith (curry SQL.VEAnd) valExprA valExprB
            match (ma, mb) with
            | (None, None) -> failwithf "Impossible JOIN of two constrained entities: %O" expr
            | (Some other, None)
            | (None, Some other) ->
                (Some <| useNewInValueExpr join.Condition, name, Some other)
            | (Some a, Some b) ->
                let ret =
                    SQL.FJoin
                        { Type = join.Type
                          A = a
                          B = b
                          Condition = useNewInValueExpr join.Condition
                        }
                (valExpr, name, Some ret)
        | SQL.FSubExpr (alias, q) -> failwith "Unexpected subexpression"

    member this.UseNewInSelectExpr expr = useNewInSelectExpr expr

let buildOuterCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (outerFields : Set<FieldName>) (check : ResolvedFieldExpr) : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let constr = Map.find constrRef.Name entity.CheckConstraints

    let fixedCheck = replaceEntityRefInExpr (Some <| relaxEntityRef entity.Root) check

    let result =
        { Attributes = Map.empty
          Result = fixedCheck
          Alias = None
        }
    let singleSelect =
        { Attributes = Map.empty
          Results = [|  QRExpr result |]
          From = Some (FEntity (None, relaxEntityRef entity.Root))
          Where = None
          GroupBy = [||]
          OrderLimit = emptyOrderLimitClause
          Extra = ObjectMap.empty
        } : ResolvedSingleSelectExpr
    let select = { CTEs = None; Tree = SSelect singleSelect; Extra = ObjectMap.empty }
    let (arguments, compiled) = compileSelectExpr layout emptyArguments select

    let replacer = ConstraintUseNewConverter(entity.Root)
    let compiled = replacer.UseNewInSelectExpr compiled

    let raiseCall =
        { Level = PLPgSQL.RLException
          Message = None
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VEValue (SQL.VString "integrity_constraint_violation"))
                  (PLPgSQL.ROTable, SQL.VEValue (constrRef.Entity.Name |> string |> SQL.VString))
                  (PLPgSQL.ROSchema, SQL.VEValue (constrRef.Entity.Schema |> string |> SQL.VString))
                ]
        } : PLPgSQL.RaiseStatement
    let checkStmt = PLPgSQL.StIfThenElse ([| (SQL.VENot (SQL.VESubquery compiled), [| PLPgSQL.StRaise raiseCall |]) |], None)

    let checkProgram =
        { Declarations = [||]
          Body = [| checkStmt; returnNullStatement |]
        } : PLPgSQL.Program
    let checkFunctionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue (SQL.SQLRawString "trigger")
          Behaviour = SQL.FBStable
          Language = PLPgSQL.plPgSQLName
          Definition = checkProgram.ToPLPgSQLString()
        } : SQL.FunctionDefinition
    let checkFunctionKey = sprintf "__out_chcon_check__%s__%s" entity.HashName constr.HashName
    let checkFunctionName = SQL.SQLName checkFunctionKey
    let checkFunctionObject = SQL.OMFunction <| Map.singleton [||] checkFunctionDefinition

    let fieldDistinctCheck name =
        let field = Map.find name entity.ColumnFields
        let checkOldColumn = SQL.VEColumn { Table = Some sqlOldRow; Name = field.ColumnName }
        let checkNewColumn = SQL.VEColumn { Table = Some sqlNewRow; Name = field.ColumnName }
        SQL.VEDistinct (checkOldColumn, checkNewColumn)

    let checkExpr =
        if Option.isNone entity.Parent then
            None
        else
            Some <| makeCheckExpr plainSubEntityColumn layout constrRef.Entity

    let updateCheck = outerFields |> Seq.map fieldDistinctCheck |> Seq.fold1 (curry SQL.VEAnd)
    let updateCheck =
        match checkExpr with
        | None -> updateCheck
        | Some check ->
            SQL.VEAnd (check, updateCheck)

    let getColumnName name = (Map.find name entity.ColumnFields).ColumnName
    let affectedColumns = outerFields |> Seq.map getColumnName |> Seq.toArray

    let schemaName = compileName entity.Root.Schema
    let tableName = compileName entity.Root.Name

    let checkUpdateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate (Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName = { Schema = Some schemaName; Name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkUpdateTriggerKey = sprintf "__out_chcon_update__%s__%s" entity.HashName constr.HashName
    let checkUpdateTriggerName = SQL.SQLName checkUpdateTriggerKey
    let checkUpdateTriggerObject = SQL.OMTrigger (tableName, checkUpdateTriggerDefinition)

    let checkInsertTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEInsert |]
          Mode = SQL.TMEachRow
          Condition = Option.map (fun check -> String.comparable check ) checkExpr
          FunctionName = { Schema = Some schemaName; Name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkInsertTriggerKey = sprintf "__out_chcon_insert__%s__%s" entity.HashName constr.HashName
    let checkInsertTriggerName = SQL.SQLName checkInsertTriggerKey
    let checkInsertTriggerObject = SQL.OMTrigger (tableName, checkInsertTriggerDefinition)

    let objects =
        [ (checkFunctionKey, (checkFunctionName, checkFunctionObject))
          (checkUpdateTriggerKey, (checkUpdateTriggerName, checkUpdateTriggerObject))
          (checkInsertTriggerKey, (checkInsertTriggerName, checkInsertTriggerObject))
        ]
        |> Map.ofSeq
    { Schemas = Map.singleton (string schemaName) { Name = schemaName; Objects = objects }
      Extensions = Set.empty
    }

// This check is built on assumption that we can collect affected rows after the operation. This is true in all cases except one: DELETE with SET NULL foreign key.
// We don't support them right now, but beware of this!
let private buildInnerCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (aggCheck : SQL.SingleSelectExpr) (key : PathTriggerKey) (trigger : PathTrigger) : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let triggerName = pathTriggerName { ConstraintRef = constrRef; Key = key }

    let aggCheck =
        { aggCheck with
              Where = Some <| Option.addWith (curry SQL.VEAnd) trigger.Expression aggCheck.Where
        }
    let checkSelect : SQL.SelectExpr =
        { CTEs = None
          Tree = SQL.SSelect aggCheck
          Extra = null
        }

    let raiseCall =
        { Level = PLPgSQL.RLException
          Message = None
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VEValue (SQL.VString "integrity_constraint_violation"))
                  (PLPgSQL.ROTable, SQL.VEValue (constrRef.Entity.Name |> string |> SQL.VString))
                  (PLPgSQL.ROSchema, SQL.VEValue (constrRef.Entity.Schema |> string |> SQL.VString))
                ]
        } : PLPgSQL.RaiseStatement
    let checkStmt = PLPgSQL.StIfThenElse ([| (SQL.VENot (SQL.VESubquery checkSelect), [| PLPgSQL.StRaise raiseCall |]) |], None)

    let checkProgram =
        { Declarations = [||]
          Body = [| checkStmt; returnNullStatement |]
        } : PLPgSQL.Program
    let checkFunctionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue (SQL.SQLRawString "trigger")
          // Otherwise changes are invisible from the trigger.
          // See https://www.postgresql.org/docs/13/trigger-datachanges.html
          Behaviour = SQL.FBVolatile
          Language = PLPgSQL.plPgSQLName
          Definition = checkProgram.ToPLPgSQLString()
        } : SQL.FunctionDefinition
    let checkFunctionKey = sprintf "__in_chcon_check__%s" triggerName
    let checkFunctionName = SQL.SQLName checkFunctionKey
    let checkFunctionObject = SQL.OMFunction <| Map.singleton [||] checkFunctionDefinition

    let getFieldInfo (fieldName, fieldEntityRef) =
        let fieldEntity = layout.FindEntity fieldEntityRef |> Option.get
        let field = Map.find fieldName fieldEntity.ColumnFields
        let checkOldColumn = SQL.VEColumn { Table = Some sqlOldRow; Name = field.ColumnName }
        let checkNewColumn = SQL.VEColumn { Table = Some sqlNewRow; Name = field.ColumnName }
        let expr = SQL.VEDistinct (checkOldColumn, checkNewColumn)
        (field.ColumnName, expr)

    let fieldsInfo = trigger.Fields |> Map.toSeq |> Seq.map getFieldInfo |> Seq.cache

    let checkExpr =
        if Option.isNone entity.Parent then
            None
        else
            Some <| makeCheckExpr plainSubEntityColumn layout constrRef.Entity

    let updateCheck = fieldsInfo |> Seq.map snd |> Seq.fold1 (curry SQL.VEAnd)
    let updateCheck =
        match checkExpr with
        | None -> updateCheck
        | Some check -> SQL.VEAnd (check, updateCheck)

    let affectedColumns = fieldsInfo |> Seq.map fst |> Seq.toArray

    let schemaName = compileName entity.Root.Schema

    // We guarantee that `Fields` is non-empty.
    let triggerSchemaName = compileName trigger.Root.Schema
    let triggerTableName = compileName trigger.Root.Name

    let checkUpdateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate (Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName = { Schema = Some schemaName; Name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkUpdateTriggerKey = sprintf "__in_chcon_update__%s" triggerName
    let checkUpdateTriggerName = SQL.SQLName checkUpdateTriggerKey
    let checkUpdateTriggerObject = SQL.OMTrigger (triggerTableName, checkUpdateTriggerDefinition)

    let functionObjects = Map.singleton checkFunctionKey (checkFunctionName, checkFunctionObject)
    let functionDbMeta : SQL.DatabaseMeta =
        { Schemas = Map.singleton (string schemaName) { Name = schemaName; Objects = functionObjects }
          Extensions = Set.empty
        }
    let triggerObjects = Map.singleton checkUpdateTriggerKey (checkUpdateTriggerName, checkUpdateTriggerObject)
    let triggerDbMeta : SQL.DatabaseMeta =
        { Schemas = Map.singleton (string triggerSchemaName) { Name = triggerSchemaName; Objects = triggerObjects }
          Extensions = Set.empty
        }

    SQL.unionDatabaseMeta functionDbMeta triggerDbMeta

let buildCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (check : ResolvedFieldExpr) : SQL.DatabaseMeta =
    let builder = CheckConstraintAffectedBuilder(layout, constrRef.Entity)
    let outerFields = builder.FindOuterFields check
    let triggers = builder.FindCheckConstraintAffected check

    // First pair of triggers: outer, e.g., check when the row itself is updated.
    let outerMeta = buildOuterCheckConstraintAssertion layout constrRef outerFields check
    // Next triggers -- for each dependent field we need to check that a change doesn't violate the constraint.
    let checkExpr =
        match compileAggregateCheckConstraintCheck layout constrRef check with
        | { CTEs = None; Tree = SQL.SSelect select } -> select
        | _ -> failwith "Impossible non-single check constraint select"
    triggers
        |> Map.toSeq
        |> Seq.map (fun (key, trig) -> buildInnerCheckConstraintAssertion layout constrRef checkExpr key trig)
        |> Seq.fold SQL.unionDatabaseMeta outerMeta

let buildAssertionsMeta (layout : Layout) (asserts : LayoutAssertions) : SQL.DatabaseMeta =
    let mapFieldOfType fieldOfType = buildColumnOfTypeAssertion layout fieldOfType.FromField fieldOfType.ToEntity
    let colOfTypes = asserts.ColumnOfTypeAssertions |> Set.toSeq |> Seq.map mapFieldOfType

    let mapCheck (key, expr) = buildCheckConstraintAssertion layout key expr
    let checkConstrs = asserts.CheckConstraints |> Map.toSeq |> Seq.map mapCheck

    Seq.concat [colOfTypes; checkConstrs] |> Seq.fold SQL.unionDatabaseMeta SQL.emptyDatabaseMeta

let private compileColumnOfTypeCheck (layout : Layout) (fromFieldRef : ResolvedFieldRef) (toEntityRef : ResolvedEntityRef) : SQL.SelectExpr =
    let entity = layout.FindEntity fromFieldRef.Entity |> Option.get
    let field = Map.find fromFieldRef.Name entity.ColumnFields
    let refEntity = layout.FindEntity toEntityRef |> Option.get

    let joinName = SQL.SQLName "__joined"
    let joinAlias = { Name = joinName; Columns = None } : SQL.TableAlias
    let joinRef = { Schema = None; Name = joinName } : SQL.TableRef
    let fromRef = compileResolvedEntityRef entity.Root
    let toRef = compileResolvedEntityRef refEntity.Root
    let fromColumn = SQL.VEColumn { Table = Some fromRef; Name = field.ColumnName }
    let toColumn = SQL.VEColumn { Table = Some joinRef; Name = sqlFunId }
    let joinExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
    let join =
        SQL.FJoin
            { Type = SQL.JoinType.Left
              A = SQL.FTable (null, None, fromRef)
              B = SQL.FTable (null, Some joinAlias, toRef)
              Condition = joinExpr
            }
    let subEntityColumn = SQL.VEColumn { Table = Some joinRef; Name = sqlFunSubEntity }
    let checkExpr = makeCheckExpr subEntityColumn layout toEntityRef
    let singleSelect =
        { Columns = [| SQL.SCExpr (None, SQL.VEAggFunc (SQL.SQLName "bool_and", SQL.AEAll [| checkExpr |])) |]
          From = Some join
          Where = None
          GroupBy = [||]
          OrderLimit = SQL.emptyOrderLimitClause
          Extra = null
        } : SQL.SingleSelectExpr
    { CTEs = None
      Tree = SQL.SSelect singleSelect
      Extra = null
    }

let private runIntegrityCheck (conn : QueryConnection) (query : SQL.SelectExpr) (cancellationToken : CancellationToken) : Task =
    unitTask {
        try
            match! conn.ExecuteValueQuery (query.ToSQLString()) Map.empty cancellationToken with
            | SQL.VBool true -> ()
            | SQL.VNull -> () // No rows found
            | ret -> raisef LayoutIntegrityException "Expected `true`, got %O" ret
        with
        | :? QueryException as ex -> raisefWithInner LayoutIntegrityException ex "Query exception"
    }

let checkAssertions (conn : QueryConnection) (layout : Layout) (assertions : LayoutAssertions) (cancellationToken : CancellationToken) : Task =
    unitTask {
        for columnOfType in assertions.ColumnOfTypeAssertions do
            let query = compileColumnOfTypeCheck layout columnOfType.FromField columnOfType.ToEntity
            try
                do! runIntegrityCheck conn query cancellationToken
            with
            | :? LayoutIntegrityException as ex -> raisefWithInner LayoutIntegrityException ex "Failed to check that all %O values point to %O entries" columnOfType.FromField columnOfType.ToEntity

        for KeyValue(constrRef, expr) in assertions.CheckConstraints do
            let query = compileAggregateCheckConstraintCheck layout constrRef expr
            try
                do! runIntegrityCheck conn query cancellationToken
            with
            | :? LayoutIntegrityException as ex -> raisefWithInner LayoutIntegrityException ex "Failed to validate check constraint %O" constrRef
    }
