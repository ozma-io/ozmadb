module FunWithFlags.FunDB.Layout.Integrity

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open System.Text
open System.Data.HashFunction.CityHash

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
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
                if Option.isSome refEntity.Inheritance then
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
                let ref = { entity = entityRef; name = colName }
                yield! columnFieldAssertions ref col
            for KeyValue (constrName, constr) in entity.CheckConstraints do
                if not constr.IsLocal then
                    let ref = { entity = entityRef; name = constrName }
                    yield
                        { emptyLayoutAssertions with
                              CheckConstraints = Map.singleton ref constr.Expression
                        }
        }

    let schemaAssertions (schemaName : SchemaName) (schema : ResolvedSchema) =
        seq {
            for KeyValue (entityName, entity) in schema.Entities do
                let ref = { schema = schemaName; name = entityName }
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

let private sqlOldRow : SQL.TableRef = { schema = None; name = sqlOldName }
let private sqlNewRow : SQL.TableRef = { schema = None; name = sqlNewName }

let private sqlNewId : SQL.ColumnRef = { table = Some sqlNewRow; name = sqlFunId }
let private sqlPlainId : SQL.ColumnRef = { table = None; name = sqlFunId }

let private returnNullStatement = PLPgSQL.StReturn (SQL.VEValue SQL.VNull)

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
    let entity = layout.FindEntity fromFieldRef.entity |> Option.get
    let field = Map.find fromFieldRef.name entity.ColumnFields
    let refEntity = layout.FindEntity toEntityRef |> Option.get
    let inheritance = Option.get refEntity.Inheritance

    let toRef = compileResolvedEntityRef refEntity.Root
    let fromSchema = compileName entity.Root.schema
    let fromTable = compileName entity.Root.name
    let fromColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.ColumnName }
    let toColumn = SQL.VEColumn { table = Some toRef; name = sqlFunId }
    let whereExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
    let singleSelect =
        { Columns = [| SQL.SCExpr (None, inheritance.CheckExpr) |]
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
                  (PLPgSQL.ROColumn, SQL.VEValue (SQL.VString <| fromFieldRef.name.ToString()))
                  (PLPgSQL.ROTable, SQL.VEValue (SQL.VString <| fromFieldRef.entity.name.ToString()))
                  (PLPgSQL.ROSchema, SQL.VEValue (SQL.VString <| fromFieldRef.entity.schema.ToString()))
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

    let checkOldColumn = SQL.VEColumn { table = Some sqlOldRow; name = field.ColumnName }
    let checkNewColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.ColumnName }
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
          FunctionName = { schema = Some fromSchema; name = checkFunctionName }
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
          FunctionName = { schema = Some fromSchema; name = checkFunctionName }
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
    { Schemas = Map.singleton (fromSchema.ToString()) { Name = fromSchema; Objects = objects } }

let private convertRelatedExpr (localRef : EntityRef) : ResolvedFieldExpr -> ResolvedFieldExpr =
        let onGlobalArg (name : ArgumentName) = failwithf "Unexpected global argument %O" name
        setRelatedExprEntity onGlobalArg localRef

// Need to be public for JsonConvert
type PathTriggerKey =
    { Ref : ResolvedFieldRef
      // This is path without the last dereference; for example, triggers for foo=>bar and foo=>baz will be merged into trigger for entity E, which contains bar and baz.
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
    { Fields : Set<FieldName>
      // These should be same for any two equal keys; Fields are merged.
      Entity : ResolvedEntityRef
      Expression : SQL.ValueExpr
    }

let private unionPathTrigger (a : PathTrigger) (b : PathTrigger) : PathTrigger =
    { Fields = Set.union a.Fields b.Fields
      Entity = b.Entity
      Expression = b.Expression
    }

let private buildSinglePathTrigger (outerRef : ResolvedFieldRef) (outerInfo : ResolvedColumnField) (fieldName : FieldName) : PathTriggerKey * PathTrigger =
        let refEntityRef =
            match outerInfo.FieldType with
            | FTReference ref -> ref
            | _ -> failwith "Impossible"

        let leftRef : SQL.ColumnRef = { table = None; name = compileName outerRef.name }
        let expr = SQL.VEBinaryOp (SQL.VEColumn leftRef, SQL.BOEq, SQL.VEColumn sqlNewId)

        let key =
            { Ref = outerRef
              EntityPath = []
            }
        let trigger =
            { Fields = Set.singleton fieldName
              Entity = refEntityRef
              Expression = expr
            }
        (key, trigger)

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
    let leftRef : SQL.ColumnRef = { table = None; name = compileName outerField }
    SQL.VEInQuery (SQL.VEColumn leftRef, selectExpr)

type private CheckConstraintAffectedBuilder (layout : Layout, constrEntityRef : ResolvedEntityRef) =
    let constrEntityInfo = layout.FindEntity constrEntityRef |> Option.get

    let rec buildPathTriggers (outerRef : ResolvedFieldRef) (outerInfo : ResolvedColumnField) : FieldName list -> (PathTriggerKey * PathTrigger) list = function
        | [] -> []
        | [fieldName] -> [buildSinglePathTrigger outerRef outerInfo fieldName]
        | fieldName :: refs ->
            let (outerKey, outerTrigger) as outerPair = buildSinglePathTrigger outerRef outerInfo fieldName
            let ref1Entity = layout.FindEntity outerTrigger.Entity |> Option.get
            let ref1Field = Map.find fieldName ref1Entity.ColumnFields

            let innerFieldRef = { entity = outerTrigger.Entity; name = fieldName }
            let innerTriggers = buildPathTriggers innerFieldRef ref1Field refs

            let expandPathTrigger (key, trigger : PathTrigger) =
                let newKey =
                    { Ref = outerRef
                      EntityPath = fieldName :: key.EntityPath
                    }
                let newTrigger =
                    { trigger with
                          Expression = expandPathTriggerExpr outerRef.name outerTrigger.Entity trigger.Expression
                    }
                (newKey, newTrigger)
            let wrappedTriggers = List.map expandPathTrigger innerTriggers
            outerPair :: wrappedTriggers

    // Returns encountered outer fields, and also path triggers.
    let findCheckConstraintAffected (expr : ResolvedFieldExpr) : Set<FieldName> * Map<PathTriggerKey, PathTrigger> =
        let mutable triggers = Map.empty
        let mutable outerFields = Set.empty

        let iterReference (ref : LinkedBoundFieldRef) =
            let outerName =
                match ref.Ref with
                | VRColumn r -> r.Ref.name
                | VRPlaceholder p -> failwithf "Unexpected placeholder %O in check expression" p
            outerFields <- Set.add outerName outerFields
            if not <| Array.isEmpty ref.Path then
                let outerRef = { entity = constrEntityRef; name = outerName }
                let outerInfo = Map.find outerName constrEntityInfo.ColumnFields
                let newTriggers = buildPathTriggers outerRef outerInfo (Array.toList ref.Path)

                let compiledRef = compileRenamedResolvedEntityRef constrEntityRef
                let postprocessTrigger trig =
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
                        match ref.table with
                        | None -> { ref with table = Some compiledRef }
                        | Some _ -> ref
                    let mapper = { SQL.idValueExprMapper with ColumnReference = mapRef }
                    let newExpr = SQL.mapValueExpr mapper trig.Expression
                    { trig with Expression = newExpr }
                
                triggers <- List.fold (fun trigs (key, trig) -> Map.addWith unionPathTrigger key (postprocessTrigger trig) trigs) triggers newTriggers
        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference
            }
        iterFieldExpr mapper expr
        
        (outerFields, triggers)

    member this.FindCheckConstraintAffected expr = findCheckConstraintAffected expr

let private compileAggregateCheckConstraintCheck (layout : Layout) (constrRef : ResolvedConstraintRef) (check : ResolvedFieldExpr) : SQL.SelectExpr =
    let entity = layout.FindEntity constrRef.entity |> Option.get
    let fixedCheck = convertRelatedExpr (relaxEntityRef entity.Root) check
    let aggExpr = FEAggFunc (FunQLName "bool_and", AEAll [| fixedCheck |])

    let result = 
        { Attributes = Map.empty
          Result = QRExpr (None, aggExpr)
        }
    let singleSelect =
        { Attributes = Map.empty
          Results = [| result |]
          From = Some (FEntity (None, relaxEntityRef entity.Root))
          Where = None
          GroupBy = [||]
          OrderLimit = emptyOrderLimitClause
          Extra = null
        } : ResolvedSingleSelectExpr
    let select = { CTEs = None; Tree = SSelect singleSelect; Extra = null }
    compileSelectExpr layout select

// Replaces entity references with `new` in simple cases.
type private ConstraintUseNewConverter (constrEntityRef : ResolvedEntityRef) =
    let compiledConstrTableRef = compileRenamedResolvedEntityRef constrEntityRef

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
        let (extraWhere, from) = Option.map useNewInFromExpr query.From |> Option.defaultValue (None, None)
        let currWhere = Option.map useNewInValueExpr query.Where
        { Columns = Array.map useNewInSelectedColumn query.Columns
          From = from
          Where = Option.unionWith (curry SQL.VEAnd) currWhere extraWhere
          GroupBy = Array.map useNewInValueExpr query.GroupBy
          OrderLimit = useNewInOrderLimitClause query.OrderLimit
          Extra = query.Extra
        }

    and useNewInOrderLimitClause (clause : SQL.OrderLimitClause) : SQL.OrderLimitClause =
        { Limit = Option.map useNewInValueExpr clause.Limit
          Offset = Option.map useNewInValueExpr clause.Offset
          OrderBy = Array.map (fun (ord, expr) -> (ord, useNewInValueExpr expr)) clause.OrderBy
        }

    and useNewInSelectedColumn : SQL.SelectedColumn -> SQL.SelectedColumn = function
        | SQL.SCAll _ -> failwith "Unexpected SELECT *"
        | SQL.SCExpr (name, expr) -> SQL.SCExpr (name, useNewInValueExpr expr)

    and useNewInValueExpr =
        let mapColumnReference (columnRef : SQL.ColumnRef) =
            match columnRef.table with
            | Some tref when tref = compiledConstrTableRef ->
                // We guarantee that NEW and OLD are unused, because all entities are renamed to `schema__name` form and there should be no aliases in
                // this expression.
                { columnRef with table = Some sqlNewRow }
            | _ -> columnRef

        let mapper =
            { SQL.idValueExprMapper with
                  ColumnReference = mapColumnReference
                  Query = fun _ -> failwith "Unexpected query expression in constraint"
            }
        SQL.mapValueExpr mapper

    // ValueExpr returned is a piece that goes into WHERE clause.
    and useNewInFromExpr : SQL.FromExpr -> (SQL.ValueExpr option * SQL.FromExpr option) = function
        | SQL.FTable (extra, pun, entity) as from ->
            let entityRef = (extra :?> RealEntityAnnotation).RealEntity
            if entityRef = constrEntityRef then
                (None, None)
            else
                (None, Some from)
        | SQL.FJoin join ->
            let (valExprA, ma) = useNewInFromExpr join.A
            let (valExprB, mb) = useNewInFromExpr join.B
            let valExpr = Option.unionWith (curry SQL.VEAnd) valExprA valExprB
            match (ma, mb) with
            | (None, None) -> failwith "Impossible JOIN of two constrained entities"
            | (Some other, None)
            | (None, Some other) ->
                (Some <| useNewInValueExpr join.Condition, Some other)
            | (Some a, Some b) ->
                let ret =
                    SQL.FJoin
                        { Type = join.Type
                          A = a
                          B = b
                          Condition = useNewInValueExpr join.Condition
                        }
                (valExpr, Some ret)
        | SQL.FSubExpr (alias, q) as subexpr ->
            match q.Extra with
            | :? RealEntityAnnotation as ann ->
                if ann.RealEntity = constrEntityRef then
                    (None, None)
                else
                    (None, Some subexpr)
            | _ -> failwith "Unexpected subexpression"

    member this.UseNewInSelectExpr expr = useNewInSelectExpr expr

let buildOuterCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (outerFields : Set<FieldName>) (check : ResolvedFieldExpr) : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.entity |> Option.get
    let constr = Map.find constrRef.name entity.CheckConstraints

    let fixedCheck = convertRelatedExpr (relaxEntityRef entity.Root) check

    let result = 
        { Attributes = Map.empty
          Result = QRExpr (None, fixedCheck)
        }
    let singleSelect =
        { Attributes = Map.empty
          Results = [| result |]
          From = Some (FEntity (None, relaxEntityRef entity.Root))
          Where = None
          GroupBy = [||]
          OrderLimit = emptyOrderLimitClause
          Extra = null
        } : ResolvedSingleSelectExpr
    let select = { CTEs = None; Tree = SSelect singleSelect; Extra = null }
    let compiled = compileSelectExpr layout select

    let replacer = ConstraintUseNewConverter(entity.Root)
    let compiled = replacer.UseNewInSelectExpr compiled

    let raiseCall =
        { Level = PLPgSQL.RLException
          Message = None
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VEValue (SQL.VString "integrity_constraint_violation"))
                  (PLPgSQL.ROTable, SQL.VEValue (constrRef.entity.name |> string |> SQL.VString))
                  (PLPgSQL.ROSchema, SQL.VEValue (constrRef.entity.schema |> string |> SQL.VString))
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
        let checkOldColumn = SQL.VEColumn { table = Some sqlOldRow; name = field.ColumnName }
        let checkNewColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.ColumnName }
        SQL.VEDistinct (checkOldColumn, checkNewColumn)

    let updateCheck = outerFields |> Seq.map fieldDistinctCheck |> Seq.fold1 (curry SQL.VEAnd)
    let updateCheck =
        match entity.Inheritance with
        | None -> updateCheck
        | Some inher -> SQL.VEAnd (inher.CheckExpr, updateCheck)
    
    let getColumnName name = (Map.find name entity.ColumnFields).ColumnName
    let affectedColumns = outerFields |> Seq.map getColumnName |> Seq.toArray

    let schemaName = compileName entity.Root.schema
    let tableName = compileName entity.Root.name

    let checkUpdateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate (Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName = { schema = Some schemaName; name = checkFunctionName }
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
          Condition = Option.map (fun inher -> String.comparable inher.CheckExpr ) entity.Inheritance
          FunctionName = { schema = Some schemaName; name = checkFunctionName }
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
    { Schemas = Map.singleton (string schemaName) { Name = schemaName; Objects = objects } }

// This check is built on assumption that we can collect affected rows after the operation. This is true in all cases except one: DELETE with SET NULL foreign key.
// We don't support them right now, but beware of this!
let private buildInnerCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (aggCheck : SQL.SingleSelectExpr) (key : PathTriggerKey) (trigger : PathTrigger) : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.entity |> Option.get
    let triggerName = pathTriggerName { ConstraintRef = constrRef; Key = key }
    let triggerEntity = layout.FindEntity trigger.Entity |> Option.get

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
                  (PLPgSQL.ROTable, SQL.VEValue (constrRef.entity.name |> string |> SQL.VString))
                  (PLPgSQL.ROSchema, SQL.VEValue (constrRef.entity.schema |> string |> SQL.VString))
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

    let fieldDistinctCheck name =
        let field = Map.find name triggerEntity.ColumnFields
        let checkOldColumn = SQL.VEColumn { table = Some sqlOldRow; name = field.ColumnName }
        let checkNewColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.ColumnName }
        SQL.VEDistinct (checkOldColumn, checkNewColumn)

    let updateCheck = trigger.Fields |> Seq.map fieldDistinctCheck |> Seq.fold1 (curry SQL.VEAnd)
    let updateCheck =
        match entity.Inheritance with
        | None -> updateCheck
        | Some inher -> SQL.VEAnd (inher.CheckExpr, updateCheck)
    
    let getColumnName name = (Map.find name triggerEntity.ColumnFields).ColumnName
    let affectedColumns = trigger.Fields |> Seq.map getColumnName |> Seq.toArray

    let schemaName = compileName entity.Root.schema
    let triggerSchemaName = compileName triggerEntity.Root.schema
    let triggerTableName = compileName triggerEntity.Root.name

    let checkUpdateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate (Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName = { schema = Some schemaName; name = checkFunctionName }
          FunctionArgs = [||]
        } : SQL.TriggerDefinition
    let checkUpdateTriggerKey = sprintf "__in_chcon_update__%s" triggerName
    let checkUpdateTriggerName = SQL.SQLName checkUpdateTriggerKey
    let checkUpdateTriggerObject = SQL.OMTrigger (triggerTableName, checkUpdateTriggerDefinition)

    let functionObjects = Map.singleton checkFunctionKey (checkFunctionName, checkFunctionObject)
    let functionDbMeta : SQL.DatabaseMeta = { Schemas = Map.singleton (string schemaName) { Name = schemaName; Objects = functionObjects } }
    let triggerObjects = Map.singleton checkUpdateTriggerKey (checkUpdateTriggerName, checkUpdateTriggerObject)
    let triggerDbMeta : SQL.DatabaseMeta = { Schemas = Map.singleton (string triggerSchemaName) { Name = triggerSchemaName; Objects = triggerObjects } }

    SQL.unionDatabaseMeta functionDbMeta triggerDbMeta

let buildCheckConstraintAssertion (layout : Layout) (constrRef : ResolvedConstraintRef) (check : ResolvedFieldExpr) : SQL.DatabaseMeta =
    let builder = CheckConstraintAffectedBuilder(layout, constrRef.entity)
    let (outerFields, triggers) = builder.FindCheckConstraintAffected check

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
    let entity = layout.FindEntity fromFieldRef.entity |> Option.get
    let field = Map.find fromFieldRef.name entity.ColumnFields
    let refEntity = layout.FindEntity toEntityRef |> Option.get
    let inheritance = Option.get refEntity.Inheritance

    let joinName = SQL.SQLName "__joined"
    let joinAlias = { Name = joinName; Columns = None } : SQL.TableAlias
    let joinRef = { schema = None; name = joinName } : SQL.TableRef
    let fromRef = compileResolvedEntityRef entity.Root
    let toRef = compileResolvedEntityRef refEntity.Root
    let fromColumn = SQL.VEColumn { table = Some fromRef; name = field.ColumnName }
    let toColumn = SQL.VEColumn { table = Some joinRef; name = sqlFunId }
    let joinExpr = SQL.VEBinaryOp (fromColumn, SQL.BOEq, toColumn)
    let join =
        SQL.FJoin
            { Type = SQL.JoinType.Left
              A = SQL.FTable (null, None, fromRef)
              B = SQL.FTable (null, Some joinAlias, toRef)
              Condition = joinExpr
            }
    let subEntityRef = { table = Some joinRef; name = sqlFunSubEntity } : SQL.ColumnRef
    let checkExpr = replaceColumnRefs subEntityRef inheritance.CheckExpr
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
