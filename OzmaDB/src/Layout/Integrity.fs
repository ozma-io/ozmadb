module OzmaDB.Layout.Integrity

open FSharpPlus
open System.Threading
open System.Threading.Tasks
open Newtonsoft.Json
open System.Text
open System.Data.HashFunction.CityHash

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Compile
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.AST
open OzmaDB.SQL.Query

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.DDL
module SQL = OzmaDB.SQL.Rename
module PLPgSQL = OzmaDB.SQL.PLPgSQL

type LayoutIntegrityException(message: string, innerException: exn, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: exn) =
        LayoutIntegrityException(message, innerException, isUserException innerException)

    new(message: string) = LayoutIntegrityException(message, null, true)

type ReferenceOfTypeAssertion =
    { FromField: ResolvedFieldRef
      ToEntity: ResolvedEntityRef }

type LayoutAssertions =
    { ReferenceOfTypeAssertions: Set<ReferenceOfTypeAssertion>
      CheckConstraints: Map<ResolvedConstraintRef, ResolvedFieldExpr>
      MaterializedFields: Map<ResolvedFieldRef, ResolvedFieldExpr> }

let unionLayoutAssertions (a: LayoutAssertions) (b: LayoutAssertions) : LayoutAssertions =
    { ReferenceOfTypeAssertions = Set.union a.ReferenceOfTypeAssertions b.ReferenceOfTypeAssertions
      CheckConstraints = Map.union a.CheckConstraints b.CheckConstraints
      MaterializedFields = Map.union a.MaterializedFields b.MaterializedFields }

let differenceLayoutAssertions (a: LayoutAssertions) (b: LayoutAssertions) : LayoutAssertions =
    { ReferenceOfTypeAssertions = Set.difference a.ReferenceOfTypeAssertions b.ReferenceOfTypeAssertions
      CheckConstraints =
        Map.differenceWithValues (fun name a b -> string a = string b) a.CheckConstraints b.CheckConstraints
      MaterializedFields =
        Map.differenceWithValues (fun name a b -> string a = string b) a.MaterializedFields b.MaterializedFields }

let emptyLayoutAssertions: LayoutAssertions =
    { ReferenceOfTypeAssertions = Set.empty
      CheckConstraints = Map.empty
      MaterializedFields = Map.empty }

let private expandMaterializedField
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (comp: ResolvedComputedField)
    : ResolvedFieldExpr =
    let cases = computedFieldCases layout ObjectMap.empty fieldRef comp |> Seq.toArray

    if Array.isEmpty cases then
        FEValue FNull
    else if Array.length cases = 1 then
        let (case, comp) = cases.[0]
        comp.Expression
    else
        let subEntityPlainRef =
            { Entity = Some <| relaxEntityRef fieldRef.Entity
              Name = funSubEntity }
            : FieldRef

        let subEntityRef =
            makeColumnReference layout (simpleColumnMeta fieldRef.Entity) subEntityPlainRef

        let buildCase (case: VirtualFieldCase, comp: ResolvedComputedField) =
            let info = { PossibleEntities = PEList case.PossibleEntities }: SubEntityMeta
            let extra = ObjectMap.singleton info

            let subEntity =
                { Ref = relaxEntityRef case.Ref
                  Extra = extra }
                : SubEntityRef

            let check = FEInheritedFrom(subEntityRef, subEntity)
            (check, comp.Expression)

        let switchCases =
            cases |> Seq.take (Array.length cases - 1) |> Seq.map buildCase |> Seq.toArray

        let (lastCase, lastComp) = cases.[Array.length cases - 1]
        FECase(switchCases, Some lastComp.Expression)

type private AssertionsBuilder(layout: Layout) =
    let columnFieldAssertions (fieldRef: ResolvedFieldRef) (field: ResolvedColumnField) : LayoutAssertions seq =
        seq {
            match field.FieldType with
            | FTScalar(SFTReference(toRef, opts)) ->
                let refEntity = layout.FindEntity toRef |> Option.get

                if Option.isSome refEntity.Parent then
                    let assertion =
                        { FromField = fieldRef
                          ToEntity = toRef }

                    yield
                        { emptyLayoutAssertions with
                            ReferenceOfTypeAssertions = Set.singleton assertion }
            | _ -> ()
        }

    let entityAssertions (entityRef: ResolvedEntityRef) (entity: ResolvedEntity) : LayoutAssertions seq =
        seq {
            for KeyValue(colName, col) in entity.ColumnFields do
                let ref = { Entity = entityRef; Name = colName }
                yield! columnFieldAssertions ref col

            for KeyValue(compName, maybeComp) in entity.ComputedFields do
                match maybeComp with
                | Ok({ IsMaterialized = true
                       Root = Some { Flags = flags }
                       InheritedFrom = None } as comp) when not <| exprIsLocal flags ->
                    let ref = { Entity = entityRef; Name = compName }

                    yield
                        { emptyLayoutAssertions with
                            MaterializedFields = Map.singleton ref (expandMaterializedField layout ref comp) }
                | _ -> ()

            for KeyValue(constrName, constr) in entity.CheckConstraints do
                if not constr.IsLocal then
                    let ref =
                        { Entity = entityRef
                          Name = constrName }

                    yield
                        { emptyLayoutAssertions with
                            CheckConstraints = Map.singleton ref constr.Expression }
        }

    let schemaAssertions (schemaName: SchemaName) (schema: ResolvedSchema) =
        seq {
            for KeyValue(entityName, entity) in schema.Entities do
                let ref =
                    { Schema = schemaName
                      Name = entityName }

                yield! entityAssertions ref entity
        }

    let layoutAssertions (subLayout: Layout) =
        seq {
            for KeyValue(schemaName, schema) in subLayout.Schemas do
                yield! schemaAssertions schemaName schema
        }

    member this.BuildAssertions subLayout = layoutAssertions subLayout

let buildAssertions (layout: Layout) (subLayout: Layout) : LayoutAssertions =
    let builder = AssertionsBuilder layout

    builder.BuildAssertions subLayout
    |> Seq.fold unionLayoutAssertions emptyLayoutAssertions

let private sqlOldName = SQL.SQLName "old"
let private sqlNewName = SQL.SQLName "new"

let private sqlOldRow: SQL.TableRef = { Schema = None; Name = sqlOldName }
let private sqlNewRow: SQL.TableRef = { Schema = None; Name = sqlNewName }

let private sqlNewId: SQL.ColumnRef =
    { Table = Some sqlNewRow
      Name = sqlFunId }

let private sqlPlainId: SQL.ColumnRef = { Table = None; Name = sqlFunId }

let private returnNullStatement = PLPgSQL.StReturn SQL.nullExpr

let private plainSubEntityColumn =
    SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

let private newSubEntityColumn =
    SQL.VEColumn
        { Table = Some sqlNewRow
          Name = sqlFunSubEntity }

let buildReferenceOfTypeAssertion
    (layout: Layout)
    (fromFieldRef: ResolvedFieldRef)
    (toEntityRef: ResolvedEntityRef)
    : SQL.DatabaseMeta =
    match makeCheckExpr plainSubEntityColumn layout toEntityRef with
    | None -> SQL.emptyDatabaseMeta
    | Some checkExpr ->
        let entity = layout.FindEntity fromFieldRef.Entity |> Option.get
        let field = Map.find fromFieldRef.Name entity.ColumnFields
        let refEntity = layout.FindEntity toEntityRef |> Option.get
        let toRef = compileResolvedEntityRef refEntity.Root
        let fromSchema = compileName entity.Root.Schema
        let fromTable = compileName entity.Root.Name

        let fromColumn =
            SQL.VEColumn
                { Table = Some sqlNewRow
                  Name = field.ColumnName }

        let toColumn = SQL.VEColumn { Table = Some toRef; Name = sqlFunId }
        let whereExpr = SQL.VEBinaryOp(fromColumn, SQL.BOEq, toColumn)
        let fTable = SQL.fromTable toRef
        let constraintName = foreignConstraintSQLName entity.HashName field.HashName

        let singleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = [| SQL.SCExpr(None, checkExpr) |]
                From = Some <| SQL.FTable fTable
                Where = Some whereExpr }

        let raiseCall =
            { Level = PLPgSQL.RLException
              Message =
                Some
                    { Format = sprintf "Value in %O is not of type %O" fromFieldRef toEntityRef
                      Options = [||] }
              Options =
                Map.ofList
                    [ (PLPgSQL.ROErrcode, "23503" |> SQL.VString |> SQL.VEValue)
                      (PLPgSQL.ROColumn, fromFieldRef.Name |> string |> SQL.VString |> SQL.VEValue)
                      (PLPgSQL.ROTable, fromFieldRef.Entity.Name |> string |> SQL.VString |> SQL.VEValue)
                      (PLPgSQL.ROSchema, fromFieldRef.Entity.Schema |> string |> SQL.VString |> SQL.VEValue)
                      (PLPgSQL.ROConstraint, constraintName |> string |> SQL.VString |> SQL.VEValue) ] }
            : PLPgSQL.RaiseStatement

        let selectExpr: SQL.SelectExpr =
            { CTEs = None
              Tree = SQL.SSelect singleSelect
              Extra = null }

        let checkStmt =
            PLPgSQL.StIfThenElse([| (SQL.VENot(SQL.VESubquery selectExpr), [| PLPgSQL.StRaise raiseCall |]) |], None)

        let checkProgram =
            { Declarations = [||]
              Body = [| checkStmt; returnNullStatement |] }
            : PLPgSQL.Program

        let functionDefinition =
            { Arguments = [||]
              ReturnValue = SQL.FRValue(SQL.SQLRawString "trigger")
              Behaviour = SQL.FBStable
              Language = PLPgSQL.plPgSQLName
              Definition = checkProgram.ToPLPgSQLString() }
            : SQL.FunctionDefinition

        let functionKey =
            sprintf "__ref_type_check__%O__%O__%O" fromFieldRef.Entity.Schema fromFieldRef.Entity.Name fromFieldRef.Name

        let functionName =
            SQL.SQLName <| sprintf "__ref_type_check__%s__%s" entity.HashName field.HashName

        let functionOverloads = Map.singleton [||] functionDefinition

        let checkOldColumn =
            SQL.VEColumn
                { Table = Some sqlOldRow
                  Name = field.ColumnName }

        let checkNewColumn =
            SQL.VEColumn
                { Table = Some sqlNewRow
                  Name = field.ColumnName }

        let checkUpdateTriggerCondition =
            if field.IsNullable then
                SQL.VEAnd(SQL.VEIsNotNull(checkNewColumn), SQL.VEDistinct(checkOldColumn, checkNewColumn))
            else
                SQL.VEBinaryOp(checkOldColumn, SQL.BONotEq, checkNewColumn)

        let updateTriggerDefinition =
            { IsConstraint = Some <| SQL.DCDeferrable false
              Order = SQL.TOAfter
              Events = [| SQL.TEUpdate(Some [| field.ColumnName |]) |]
              Mode = SQL.TMEachRow
              Condition = Some <| String.comparable checkUpdateTriggerCondition
              FunctionName =
                { Schema = Some fromSchema
                  Name = functionName }
              FunctionArgs = [||] }
            : SQL.TriggerDefinition

        let updateTriggerKey =
            sprintf
                "__ref_type_update__%O__%O__%O"
                fromFieldRef.Entity.Schema
                fromFieldRef.Entity.Name
                fromFieldRef.Name

        let updateTriggerName =
            SQL.SQLName
            <| sprintf "01_ref_type_update__%s__%s" entity.HashName field.HashName

        let updateTriggers =
            Seq.singleton (updateTriggerName, (Set.singleton updateTriggerKey, updateTriggerDefinition))

        let insertTriggerDefinition =
            { IsConstraint = Some <| SQL.DCDeferrable false
              Order = SQL.TOAfter
              Events = [| SQL.TEInsert |]
              Mode = SQL.TMEachRow
              Condition = None
              FunctionName =
                { Schema = Some fromSchema
                  Name = functionName }
              FunctionArgs = [||] }
            : SQL.TriggerDefinition

        let insertTriggerKey =
            sprintf
                "__ref_type_insert__%O__%O__%O"
                fromFieldRef.Entity.Schema
                fromFieldRef.Entity.Name
                fromFieldRef.Name

        let insertTriggerName =
            SQL.SQLName
            <| sprintf "01_ref_type_insert__%s__%s" entity.HashName field.HashName

        let insertTriggers =
            Seq.singleton (insertTriggerName, (Set.singleton insertTriggerKey, insertTriggerDefinition))

        let tableObjects =
            { SQL.emptyTableObjectsMeta with
                Triggers = Map.ofSeqUnique (Seq.concat [ updateTriggers; insertTriggers ]) }
            : SQL.TableObjectsMeta

        let schema =
            { SQL.emptySchemaMeta with
                Relations = Map.singleton fromTable (SQL.OMTable tableObjects)
                Functions = Map.singleton functionName (Set.singleton functionKey, functionOverloads) }
            : SQL.SchemaMeta

        { SQL.emptyDatabaseMeta with
            Schemas = Map.singleton (fromSchema) (Set.empty, schema) }

// This is path without the last dereference; for example, triggers for foo=>bar and foo=>baz will be merged into trigger for entity E, which handles bar and baz.
type PathTriggerKey = ResolvedFieldRef list

let private pathTriggerHasher =
    let config = CityHashConfig()
    config.HashSizeInBits <- 128
    CityHashFactory.Instance.Create(config)

type PathTriggerFullKey =
    { ConstraintRef: ResolvedFieldRef
      Key: PathTriggerKey }

// As we can't compress PathTriggerKey into 63 symbols (PostgreSQL restriction!) we hash it instead.
let private pathTriggerName (key: PathTriggerFullKey) =
    let pathStr = JsonConvert.SerializeObject key
    let pathBytes = Encoding.UTF8.GetBytes pathStr
    let str = pathTriggerHasher.ComputeHash(pathBytes).AsHexString()
    String.truncate 40 str

let private pathTriggerKey (key: PathTriggerFullKey) = JsonConvert.SerializeObject key

type private PathTrigger =
    { // Map from field root entities to set of field names.
      Fields: Map<ResolvedEntityRef, Set<FieldName>>
      Root: ResolvedEntityRef }

let private unionPathTrigger (a: PathTrigger) (b: PathTrigger) : PathTrigger =
    { // Values should be the same.
      Fields = Map.unionWith Set.union a.Fields b.Fields
      Root = b.Root }

let private buildPathTriggerExpression (entityRef: ResolvedEntityRef) (key: PathTriggerKey) : SQL.ValueExpr =
    let rec traverse: PathTriggerKey -> SQL.ValueExpr =
        function
        | [] -> failwith "Unexpected empty key for path trigger"
        | [ outerRef ] ->
            let leftRef: SQL.ColumnRef =
                { Table = None
                  Name = compileName outerRef.Name }

            SQL.VEBinaryOp(SQL.VEColumn leftRef, SQL.BOEq, SQL.VEColumn sqlNewId)
        | outerRef :: refs ->
            let innerExpr = traverse refs
            let fTable = SQL.fromTable <| compileResolvedEntityRef outerRef.Entity

            let singleSelectExpr =
                { SQL.emptySingleSelectExpr with
                    Columns = [| SQL.SCExpr(None, SQL.VEColumn sqlPlainId) |]
                    From = Some(SQL.FTable fTable)
                    Where = Some innerExpr }

            let selectExpr = SQL.selectExpr (SQL.SSelect singleSelectExpr)

            let leftRef: SQL.ColumnRef =
                { Table = None
                  Name = compileName outerRef.Name }

            SQL.VEInQuery(SQL.VEColumn leftRef, selectExpr)

    let expr = traverse key
    // Replace outer table-less references with named references, so that we can insert this expression
    // into compiled OzmaQL query.
    // For example:
    // > foo IN (SELECT id FROM ent WHERE bar = new.id)
    // Becomes:
    // > schema__other_ent.foo IN (SELECT id FROM ent WHERE bar = new.id)
    // Also:
    // > foo <> new.id
    // Becomes:
    // > schema__other_ent.foo <> new.id
    let compiledRef = compileRenamedResolvedEntityRef entityRef

    let mapRef (ref: SQL.ColumnRef) =
        match ref.Table with
        | None -> { ref with Table = Some compiledRef }
        | Some _ -> ref

    let mapper =
        { SQL.idValueExprMapper with
            ColumnReference = mapRef }

    SQL.mapValueExpr mapper expr


let rec private findOuterFields (layout: Layout) (expr: ResolvedFieldExpr) : Map<FieldName, ResolvedEntityRef> =
    let mutable outerFields = Map.empty

    let iterReference (ref: LinkedBoundFieldRef) =
        let info = ObjectMap.findType<FieldRefMeta> ref.Extra
        let boundInfo = getFieldRefBoundColumn info
        let field = layout.FindField boundInfo.Ref.Entity boundInfo.Ref.Name |> Option.get

        match field.Field with
        | RColumnField col -> outerFields <- Map.add field.Name boundInfo.Ref.Entity outerFields
        | RComputedField comp when comp.IsMaterialized ->
            outerFields <- Map.add field.Name boundInfo.Ref.Entity outerFields
        | RComputedField comp ->
            for (case, comp) in computedFieldCases layout ref.Extra { boundInfo.Ref with Name = field.Name } comp do
                let fields = findOuterFields layout comp.Expression
                outerFields <- Map.union outerFields fields
        | _ -> ()

    let mapper =
        { idFieldExprIter with
            FieldReference = iterReference }

    iterFieldExpr mapper expr

    outerFields

type private AffectedByExprBuilder(layout: Layout, constrEntityRef: ResolvedEntityRef) =
    let rec buildOuterPathTrigger (extra: ObjectMap) (fieldRef: ResolvedFieldRef) : (PathTriggerKey * PathTrigger) seq =
        let refEntity = layout.FindEntity fieldRef.Entity |> Option.get
        let refField = refEntity.FindField fieldRef.Name |> Option.get

        let columnFieldTrigger () : (PathTriggerKey * PathTrigger) seq =
            let trigger =
                { Fields = Map.singleton fieldRef.Entity (Set.singleton refField.Name)
                  Root = refEntity.Root }

            Seq.singleton ([], trigger)

        match refField.Field with
        | RId
        // FIXME: We will allow to change subentities later.
        | RSubEntity -> Seq.empty
        | RColumnField col -> columnFieldTrigger ()
        | RComputedField comp when comp.IsMaterialized -> columnFieldTrigger ()
        | RComputedField comp ->
            computedFieldCases layout extra { fieldRef with Name = refField.Name } comp
            |> Seq.map (fun (case, comp) -> comp.Expression)
            |> Seq.collect buildExprTriggers

    and buildPathTriggers
        (extra: ObjectMap)
        (outerRef: ResolvedFieldRef)
        : (ResolvedEntityRef * PathArrow) list -> (PathTriggerKey * PathTrigger) seq =
        function
        | [] -> buildOuterPathTrigger extra outerRef
        | (entityRef, arrow) :: refs ->
            let outerTriggers = buildOuterPathTrigger extra outerRef
            let refEntity = layout.FindEntity entityRef |> Option.get
            let refField = refEntity.FindField arrow.Name |> Option.get

            let innerFieldRef =
                { Entity = entityRef
                  Name = refField.Name }

            let innerTriggers = buildPathTriggers extra innerFieldRef refs

            let expandPathTrigger (key: PathTriggerKey, trigger: PathTrigger) = (outerRef :: key, trigger)
            let wrappedTriggers = Seq.map expandPathTrigger innerTriggers
            Seq.append outerTriggers wrappedTriggers

    and buildExprTriggers (expr: ResolvedFieldExpr) : (PathTriggerKey * PathTrigger) seq =
        let mutable triggers = Seq.empty

        let iterReference (ref: LinkedBoundFieldRef) =
            let info = ObjectMap.findType<FieldRefMeta> ref.Extra
            let boundInfo = getFieldRefBoundColumn info
            let pathWithEntities = Seq.zip info.Path ref.Ref.Path |> List.ofSeq
            let newTriggers = buildPathTriggers ref.Extra boundInfo.Ref pathWithEntities
            triggers <- Seq.append triggers newTriggers

        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference }

        iterFieldExpr mapper expr

        triggers

    let finalizeTriggers (triggers: (PathTriggerKey * PathTrigger) seq) : Map<PathTriggerKey, PathTrigger> =
        triggers
        |> Seq.filter (fun (key, trig) -> not (List.isEmpty key))
        |> Seq.fold (fun trigs (key, trig) -> Map.addWith unionPathTrigger key trig trigs) Map.empty

    // Returns encountered outer fields, and also path triggers.
    let findAffectedByExpr (expr: ResolvedFieldExpr) : Map<PathTriggerKey, PathTrigger> =
        buildExprTriggers expr |> finalizeTriggers

    member this.FindAffectedByExpr expr = findAffectedByExpr expr

// Replaces entity references with `new` in simple cases.
type private ConstraintUseNewConverter(constrEntityRef: ResolvedEntityRef) =
    let compiledConstrTableName = renameResolvedEntityRef constrEntityRef

    let rec useNewInSelectTreeExpr: SQL.SelectTreeExpr -> SQL.SelectTreeExpr =
        function
        | SQL.SSelect query -> SQL.SSelect <| useNewInSingleSelectExpr query
        | SQL.SSetOp setOp ->
            SQL.SSetOp
                { Operation = setOp.Operation
                  AllowDuplicates = setOp.AllowDuplicates
                  A = useNewInSelectExpr setOp.A
                  B = useNewInSelectExpr setOp.B
                  OrderLimit = useNewInOrderLimitClause setOp.OrderLimit }
        | SQL.SValues values -> SQL.SValues values

    and useNewInSelectExpr (select: SQL.SelectExpr) : SQL.SelectExpr =
        if Option.isSome select.CTEs then
            failwith "Impossible"

        { CTEs = None
          Tree = useNewInSelectTreeExpr select.Tree
          Extra = null }

    and useNewInSingleSelectExpr (query: SQL.SingleSelectExpr) : SQL.SingleSelectExpr =
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
          Locking = query.Locking
          Extra = query.Extra }

    and useNewInOrderColumn (col: SQL.OrderColumn) : SQL.OrderColumn =
        { Expr = useNewInValueExpr col.Expr
          Order = col.Order
          Nulls = col.Nulls }

    and useNewInOrderLimitClause (clause: SQL.OrderLimitClause) : SQL.OrderLimitClause =
        { Limit = Option.map useNewInValueExpr clause.Limit
          Offset = Option.map useNewInValueExpr clause.Offset
          OrderBy = Array.map useNewInOrderColumn clause.OrderBy }

    and useNewInSelectedColumn: SQL.SelectedColumn -> SQL.SelectedColumn =
        function
        | SQL.SCAll _ -> failwith "Unexpected SELECT *"
        | SQL.SCExpr(name, expr) -> SQL.SCExpr(name, useNewInValueExpr expr)

    and useNewInValueExpr =
        let renamesMap = Map.singleton compiledConstrTableName sqlNewName
        SQL.naiveRenameTablesExpr renamesMap

    // ValueExpr returned is a piece that goes into WHERE clause.
    and useNewInFromExpr: SQL.FromExpr -> (SQL.ValueExpr option * SQL.TableName option * SQL.FromExpr option) =
        function
        | SQL.FTable fTable as from ->
            let ann = fTable.Extra :?> RealEntityAnnotation

            if not ann.FromPath && ann.RealEntity = constrEntityRef then
                (None, fTable.Alias |> Option.map (fun x -> x.Name), None)
            else
                (None, None, Some from)
        | SQL.FTableExpr expr -> failwith "Unexpected subexpression"
        | SQL.FJoin join as expr ->
            let (valExprA, nameA, ma) = useNewInFromExpr join.A
            let (valExprB, nameB, mb) = useNewInFromExpr join.B
            let name = Option.unionUnique nameA nameB
            let valExpr = Option.unionWith (curry SQL.VEAnd) valExprA valExprB

            match (ma, mb) with
            | (None, None) -> failwithf "Impossible JOIN of two constrained entities: %O" expr
            | (Some other, None)
            | (None, Some other) -> (Some <| useNewInValueExpr join.Condition, name, Some other)
            | (Some a, Some b) ->
                let ret =
                    SQL.FJoin
                        { Type = join.Type
                          A = a
                          B = b
                          Condition = useNewInValueExpr join.Condition }

                (valExpr, name, Some ret)

    member this.UseNewInSelectExpr expr = useNewInSelectExpr expr

let private distinctCheck (columnName: SQL.ColumnName) =
    let checkOldColumn =
        SQL.VEColumn
            { Table = Some sqlOldRow
              Name = columnName }

    let checkNewColumn =
        SQL.VEColumn
            { Table = Some sqlNewRow
              Name = columnName }

    SQL.VEDistinct(checkOldColumn, checkNewColumn)

let private buildUpdateCheck (layout: Layout) (outerFields: (FieldName * ResolvedEntityRef) seq) =
    let fieldDistinctCheck (name, entityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        let columnName = getColumnName entity name
        distinctCheck columnName

    outerFields |> Seq.map fieldDistinctCheck |> Seq.fold1 (curry SQL.VEOr)

let buildOuterCheckConstraintAssertion
    (layout: Layout)
    (constrRef: ResolvedConstraintRef)
    (check: ResolvedFieldExpr)
    : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let constr = Map.find constrRef.Name entity.CheckConstraints
    let outerFields = findOuterFields layout check

    let fixedCheck = replaceEntityRefInExpr (Some <| relaxEntityRef entity.Root) check

    let result = queryColumnResult fixedCheck
    let myFromEntity = fromEntity (relaxEntityRef entity.Root)

    let singleSelect =
        { emptySingleSelectExpr with
            Results = [| QRExpr result |]
            From = Some(FEntity myFromEntity) }

    let select = selectExpr (SSelect singleSelect)
    let (exprInfo, compiled) = compileSelectExpr layout emptyArguments select
    let constraintName = checkConstraintSQLName entity.HashName constr.HashName

    let replacer = ConstraintUseNewConverter(entity.Root)
    let compiled = replacer.UseNewInSelectExpr compiled

    let raiseCall =
        { Level = PLPgSQL.RLException
          Message =
            Some
                { Format = sprintf "Constraint %O is violated" constrRef
                  Options = [||] }
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VString "23514" |> SQL.VEValue)
                  (PLPgSQL.ROTable, constrRef.Entity.Name |> string |> SQL.VString |> SQL.VEValue)
                  (PLPgSQL.ROSchema, constrRef.Entity.Schema |> string |> SQL.VString |> SQL.VEValue)
                  (PLPgSQL.ROConstraint, constraintName |> string |> SQL.VString |> SQL.VEValue) ] }
        : PLPgSQL.RaiseStatement

    let checkStmt =
        PLPgSQL.StIfThenElse([| (SQL.VENot(SQL.VESubquery compiled), [| PLPgSQL.StRaise raiseCall |]) |], None)

    let checkProgram =
        { Declarations = [||]
          Body = [| checkStmt; returnNullStatement |] }
        : PLPgSQL.Program

    let functionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue(SQL.SQLRawString "trigger")
          Behaviour = SQL.FBStable
          Language = PLPgSQL.plPgSQLName
          Definition = checkProgram.ToPLPgSQLString() }
        : SQL.FunctionDefinition

    let functionKey =
        sprintf "__out_chcon_check__%O__%O__%O" constrRef.Entity.Schema constrRef.Entity.Name constrRef.Name

    let functionName =
        SQL.SQLName
        <| sprintf "__out_chcon_check__%s__%s" entity.HashName constr.HashName

    let functionOverloads = Map.singleton [||] functionDefinition

    let updateCheck = buildUpdateCheck layout (Map.toSeq outerFields)
    let checkExpr = makeCheckExpr newSubEntityColumn layout constrRef.Entity

    let updateCheck =
        Option.addWith (fun update check -> SQL.VEAnd(check, update)) updateCheck checkExpr

    let getFieldColumnName (name, entityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        getColumnName entity name

    let affectedColumns =
        outerFields |> Map.toSeq |> Seq.map getFieldColumnName |> Seq.toArray

    let schemaName = compileName entity.Root.Schema
    let tableName = compileName entity.Root.Name

    let updateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate(Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let updateTriggerKey =
        sprintf "__out_chcon_update__%O__%O__%O" constrRef.Entity.Schema constrRef.Entity.Name constrRef.Name

    let updateTriggerName =
        SQL.SQLName
        <| sprintf "01_out_chcon_update__%s__%s" entity.HashName constr.HashName

    let updateTriggers =
        Seq.singleton (updateTriggerName, (Set.singleton updateTriggerKey, updateTriggerDefinition))

    let insertTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEInsert |]
          Mode = SQL.TMEachRow
          Condition = Option.map (fun check -> String.comparable check) checkExpr
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let insertTriggerKey =
        sprintf "__out_chcon_insert__%O__%O__%O" constrRef.Entity.Schema constrRef.Entity.Name constrRef.Name

    let insertTriggerName =
        SQL.SQLName
        <| sprintf "01_out_chcon_insert__%s__%s" entity.HashName constr.HashName

    let insertTriggers =
        Seq.singleton (insertTriggerName, (Set.singleton insertTriggerKey, insertTriggerDefinition))

    let tableObjects =
        { SQL.emptyTableObjectsMeta with
            Triggers = Map.ofSeqUnique (Seq.concat [ updateTriggers; insertTriggers ]) }
        : SQL.TableObjectsMeta

    let schema =
        { SQL.emptySchemaMeta with
            Relations = Map.singleton tableName (SQL.OMTable tableObjects)
            Functions = Map.singleton functionName (Set.singleton functionKey, functionOverloads) }
        : SQL.SchemaMeta

    { SQL.emptyDatabaseMeta with
        Schemas = Map.singleton schemaName (Set.empty, schema) }

let private getPathTriggerAffected (layout: Layout) (trigger: PathTrigger) : SQL.ColumnName[] =
    let getFieldInfo (entity, fields) =
        let fieldEntity = layout.FindEntity entity |> Option.get
        Seq.map (getColumnName fieldEntity) fields

    trigger.Fields |> Map.toSeq |> Seq.collect getFieldInfo |> Seq.toArray

let private buildInnerCheckConstraintAssertion
    (layout: Layout)
    (constrRef: ResolvedConstraintRef)
    (aggCheck: SQL.SingleSelectExpr)
    (key: PathTriggerKey)
    (trigger: PathTrigger)
    : SQL.DatabaseMeta =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let constr = Map.find constrRef.Name entity.CheckConstraints
    let fullTriggerKey = { ConstraintRef = constrRef; Key = key }
    let triggerName = pathTriggerName fullTriggerKey
    let triggerKey = pathTriggerKey fullTriggerKey
    let constraintName = checkConstraintSQLName entity.HashName constr.HashName

    let whereExpr = buildPathTriggerExpression constrRef.Entity key

    let aggCheck =
        { aggCheck with
            Where = Some <| Option.addWith (curry SQL.VEAnd) whereExpr aggCheck.Where }

    let checkSelect: SQL.SelectExpr =
        { CTEs = None
          Tree = SQL.SSelect aggCheck
          Extra = null }

    let raiseCall =
        { Level = PLPgSQL.RLException
          Message =
            Some
                { Format = sprintf "Constraint %O is violated" constrRef
                  Options = [||] }
          Options =
            Map.ofList
                [ (PLPgSQL.ROErrcode, SQL.VString "23514" |> SQL.VEValue)
                  (PLPgSQL.ROTable, constrRef.Entity.Name |> string |> SQL.VString |> SQL.VEValue)
                  (PLPgSQL.ROSchema, constrRef.Entity.Schema |> string |> SQL.VString |> SQL.VEValue)
                  (PLPgSQL.ROConstraint, constraintName |> string |> SQL.VString |> SQL.VEValue) ] }
        : PLPgSQL.RaiseStatement

    let checkStmt =
        PLPgSQL.StIfThenElse([| (SQL.VENot(SQL.VESubquery checkSelect), [| PLPgSQL.StRaise raiseCall |]) |], None)

    let checkProgram =
        { Declarations = [||]
          Body = [| checkStmt; returnNullStatement |] }
        : PLPgSQL.Program

    let functionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue(SQL.SQLRawString "trigger")
          // Otherwise changes are invisible from the trigger.
          // See https://www.postgresql.org/docs/13/trigger-datachanges.html
          Behaviour = SQL.FBVolatile
          Language = PLPgSQL.plPgSQLName
          Definition = checkProgram.ToPLPgSQLString() }
        : SQL.FunctionDefinition

    let functionKey = sprintf "__in_chcon_check__%s" triggerKey
    let functionName = SQL.SQLName <| sprintf "__in_chcon_check__%s" triggerName
    let functionOverloads = Map.singleton [||] functionDefinition

    let affectedColumns = getPathTriggerAffected layout trigger

    let checkExpr = makeCheckExpr plainSubEntityColumn layout constrRef.Entity

    let updateCheck =
        affectedColumns |> Seq.map distinctCheck |> Seq.fold1 (curry SQL.VEOr)

    let updateCheck =
        match checkExpr with
        | None -> updateCheck
        | Some check -> SQL.VEAnd(check, updateCheck)

    let schemaName = compileName entity.Root.Schema

    // We guarantee that `Fields` is non-empty.
    let triggerSchemaName = compileName trigger.Root.Schema
    let triggerTableName = compileName trigger.Root.Name

    let updateTriggerDefinition =
        { IsConstraint = Some <| SQL.DCDeferrable false
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate(Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let updateTriggerKey = sprintf "__in_chcon_update__%s" triggerKey
    let updateTriggerName = SQL.SQLName <| sprintf "01_in_chcon_update__%s" triggerName

    let updateTriggers =
        Seq.singleton (updateTriggerName, (Set.singleton updateTriggerKey, updateTriggerDefinition))

    let triggerTableObjects =
        { SQL.emptyTableObjectsMeta with
            Triggers = Map.ofSeqUnique updateTriggers }
        : SQL.TableObjectsMeta

    let triggerSchema =
        { SQL.emptySchemaMeta with
            Relations = Map.singleton triggerTableName (SQL.OMTable triggerTableObjects) }
        : SQL.SchemaMeta

    let triggerDbMeta =
        { SQL.emptyDatabaseMeta with
            Schemas = Map.singleton triggerSchemaName (Set.empty, triggerSchema) }

    let functionSchema =
        { SQL.emptySchemaMeta with
            Functions = Map.singleton functionName (Set.empty, functionOverloads) }
        : SQL.SchemaMeta

    let functionDbMeta =
        { SQL.emptyDatabaseMeta with
            Schemas = Map.singleton schemaName (Set.empty, functionSchema) }

    SQL.unionDatabaseMeta functionDbMeta triggerDbMeta

let private compileAggregateCheckConstraintCheck
    (layout: Layout)
    (constrRef: ResolvedConstraintRef)
    (check: ResolvedFieldExpr)
    : SQL.SelectExpr =
    let entity = layout.FindEntity constrRef.Entity |> Option.get
    let fixedCheck = replaceEntityRefInExpr (Some <| relaxEntityRef entity.Root) check
    let aggExpr = FEAggFunc(OzmaQLName "bool_and", AEAll [| fixedCheck |])

    let result = queryColumnResult aggExpr
    let myFromEntity = fromEntity <| relaxEntityRef entity.Root

    let singleSelect =
        { emptySingleSelectExpr with
            Results = [| QRExpr result |]
            From = Some(FEntity myFromEntity) }

    let select = selectExpr (SSelect singleSelect)
    let (exprInfo, ret) = compileSelectExpr layout emptyArguments select
    ret

let private compileCheckConstraintBulkCheck
    (layout: Layout)
    (constrRef: ResolvedConstraintRef)
    (check: ResolvedFieldExpr)
    : SQL.SelectExpr =
    let entity = layout.FindEntity constrRef.Entity |> Option.get

    let singleSelect =
        match compileAggregateCheckConstraintCheck layout constrRef check with
        | { CTEs = None
            Tree = SQL.SSelect select } -> select
        | _ -> failwith "Impossible non-single check constraint select"

    let checkExpr = makeCheckExpr plainSubEntityColumn layout constrRef.Entity

    let singleSelect =
        { singleSelect with
            Where = Option.unionWith (curry SQL.VEAnd) checkExpr singleSelect.Where }

    { CTEs = None
      Tree = SQL.SSelect singleSelect
      Extra = null }

let buildCheckConstraintAssertion
    (layout: Layout)
    (constrRef: ResolvedConstraintRef)
    (check: ResolvedFieldExpr)
    : SQL.DatabaseMeta =
    let builder = AffectedByExprBuilder(layout, constrRef.Entity)
    let triggers = builder.FindAffectedByExpr check

    // First pair of triggers: outer, e.g., check when the row itself is updated.
    let outerMeta = buildOuterCheckConstraintAssertion layout constrRef check
    // Next triggers -- for each dependent field we need to check that a change doesn't violate the constraint.
    let checkExpr =
        match compileAggregateCheckConstraintCheck layout constrRef check with
        | { CTEs = None
            Tree = SQL.SSelect select } -> select
        | _ -> failwith "Impossible non-single check constraint select"

    triggers
    |> Map.toSeq
    |> Seq.map (fun (key, trig) -> buildInnerCheckConstraintAssertion layout constrRef checkExpr key trig)
    |> Seq.fold SQL.unionDatabaseMeta outerMeta

let buildOuterMaterializedFieldStore
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (comp: ResolvedComputedField)
    (outerFields: Map<FieldName, ResolvedEntityRef>)
    (update: SQL.UpdateExpr)
    : SQL.DatabaseMeta =
    let entity = layout.FindEntity fieldRef.Entity |> Option.get

    let tableRef = compileResolvedEntityRef entity.Root

    let updateId =
        { Table = Some tableRef
          Name = sqlFunId }
        : SQL.ColumnRef

    let idExpr = SQL.VEBinaryOp(SQL.VEColumn updateId, SQL.BOEq, SQL.VEColumn sqlNewId)

    let update =
        { update with
            Where = Option.unionWith (curry SQL.VEAnd) (Some idExpr) update.Where }

    let updateStmt = PLPgSQL.StUpdate update

    let program =
        { Declarations = [||]
          Body = [| updateStmt; returnNullStatement |] }
        : PLPgSQL.Program

    let functionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue(SQL.SQLRawString "trigger")
          Behaviour = SQL.FBVolatile
          Language = PLPgSQL.plPgSQLName
          Definition = program.ToPLPgSQLString() }
        : SQL.FunctionDefinition

    let functionKey =
        sprintf "__out_mat_store__%O__%O__%O" fieldRef.Entity.Schema fieldRef.Entity.Name fieldRef.Name

    let functionName =
        SQL.SQLName <| sprintf "__out_mat_store__%s__%s" entity.HashName comp.HashName

    let functionOverloads = Map.singleton [||] functionDefinition

    let updateCheck = buildUpdateCheck layout (Map.toSeq outerFields)
    let checkExpr = makeCheckExpr newSubEntityColumn layout fieldRef.Entity

    let updateCheck =
        Option.addWith (fun update check -> SQL.VEAnd(check, update)) updateCheck checkExpr

    let getFieldColumnName (name, entityRef) =
        let entity = layout.FindEntity entityRef |> Option.get
        getColumnName entity name

    let affectedColumns =
        outerFields |> Map.toSeq |> Seq.map getFieldColumnName |> Seq.toArray

    let schemaName = compileName entity.Root.Schema
    let tableName = compileName entity.Root.Name

    let updateTriggerDefinition =
        { IsConstraint = None
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate(Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let updateTriggerKey =
        sprintf "__out_mat_update__%O__%O__%O" fieldRef.Entity.Schema fieldRef.Entity.Name fieldRef.Name

    let updateTriggerName =
        SQL.SQLName <| sprintf "00_out_mat_update__%s__%s" entity.HashName comp.HashName

    let updateTriggers =
        Seq.singleton (updateTriggerName, (Set.singleton updateTriggerKey, updateTriggerDefinition))

    let insertTriggerDefinition =
        { IsConstraint = None
          Order = SQL.TOAfter
          Events = [| SQL.TEInsert |]
          Mode = SQL.TMEachRow
          Condition = Option.map (fun check -> String.comparable check) checkExpr
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let insertTriggerKey =
        sprintf "__out_mat_insert__%O__%O__%O" fieldRef.Entity.Schema fieldRef.Entity.Name fieldRef.Name

    let insertTriggerName =
        SQL.SQLName <| sprintf "00_out_mat_insert__%s__%s" entity.HashName comp.HashName

    let insertTriggers =
        Seq.singleton (insertTriggerName, (Set.singleton insertTriggerKey, insertTriggerDefinition))

    let tableObjects =
        { SQL.emptyTableObjectsMeta with
            Triggers = Map.ofSeqUnique (Seq.concat [ updateTriggers; insertTriggers ]) }
        : SQL.TableObjectsMeta

    let schema =
        { SQL.emptySchemaMeta with
            Relations = Map.singleton tableName (SQL.OMTable tableObjects)
            Functions = Map.singleton functionName (Set.singleton functionKey, functionOverloads) }
        : SQL.SchemaMeta

    { SQL.emptyDatabaseMeta with
        Schemas = Map.singleton schemaName (Set.empty, schema) }

// This check is built on assumption that we can collect affected rows after the operation. This is true in all cases except one: DELETE with SET NULL foreign key.
// We don't support them right now, but beware of this!
let private buildInnerMaterializedFieldStore
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (comp: ResolvedComputedField)
    (update: SQL.UpdateExpr)
    (key: PathTriggerKey)
    (trigger: PathTrigger)
    : SQL.DatabaseMeta =
    let entity = layout.FindEntity fieldRef.Entity |> Option.get
    let fullTriggerKey = { ConstraintRef = fieldRef; Key = key }
    let triggerName = pathTriggerName fullTriggerKey
    let triggerKey = pathTriggerKey fullTriggerKey

    let whereExpr = buildPathTriggerExpression fieldRef.Entity key

    let update =
        { update with
            Where = Option.unionWith (curry SQL.VEAnd) (Some whereExpr) update.Where }

    let updateStmt = PLPgSQL.StUpdate update

    let program =
        { Declarations = [||]
          Body = [| updateStmt; returnNullStatement |] }
        : PLPgSQL.Program

    let functionDefinition =
        { Arguments = [||]
          ReturnValue = SQL.FRValue(SQL.SQLRawString "trigger")
          Behaviour = SQL.FBVolatile
          Language = PLPgSQL.plPgSQLName
          Definition = program.ToPLPgSQLString() }
        : SQL.FunctionDefinition

    let functionKey = sprintf "__in_mat_store__%s" triggerKey
    let functionName = SQL.SQLName <| sprintf "__in_mat_store__%s" triggerName
    let functionOverloads = Map.singleton [||] functionDefinition

    let affectedColumns = getPathTriggerAffected layout trigger

    let updateCheck =
        affectedColumns |> Seq.map distinctCheck |> Seq.fold1 (curry SQL.VEOr)

    let checkExpr = makeCheckExpr newSubEntityColumn layout fieldRef.Entity

    let updateCheck =
        Option.addWith (fun update check -> SQL.VEAnd(check, update)) updateCheck checkExpr

    let schemaName = compileName entity.Root.Schema

    // We guarantee that `Fields` is non-empty.
    let triggerSchemaName = compileName trigger.Root.Schema
    let triggerTableName = compileName trigger.Root.Name

    let updateTriggerDefinition =
        { IsConstraint = None
          Order = SQL.TOAfter
          Events = [| SQL.TEUpdate(Some affectedColumns) |]
          Mode = SQL.TMEachRow
          Condition = Some <| String.comparable updateCheck
          FunctionName =
            { Schema = Some schemaName
              Name = functionName }
          FunctionArgs = [||] }
        : SQL.TriggerDefinition

    let updateTriggerKey = sprintf "__in_mat_update__%s" triggerKey
    let updateTriggerName = SQL.SQLName <| sprintf "00_in_mat_update__%s" triggerName

    let updateTriggers =
        Seq.singleton (updateTriggerName, (Set.singleton updateTriggerKey, updateTriggerDefinition))

    let triggerTableObjects =
        { SQL.emptyTableObjectsMeta with
            Triggers = Map.ofSeqUnique updateTriggers }
        : SQL.TableObjectsMeta

    let triggerSchema =
        { SQL.emptySchemaMeta with
            Relations = Map.singleton triggerTableName (SQL.OMTable triggerTableObjects) }
        : SQL.SchemaMeta

    let triggerDbMeta =
        { SQL.emptyDatabaseMeta with
            Schemas = Map.singleton triggerSchemaName (Set.empty, triggerSchema) }

    let functionSchema =
        { SQL.emptySchemaMeta with
            Functions = Map.singleton functionName (Set.empty, functionOverloads) }
        : SQL.SchemaMeta

    let functionDbMeta =
        { SQL.emptyDatabaseMeta with
            Schemas = Map.singleton schemaName (Set.empty, functionSchema) }

    SQL.unionDatabaseMeta functionDbMeta triggerDbMeta

let private compileMaterializedFieldUpdate
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (comp: ResolvedComputedField)
    (expr: ResolvedFieldExpr)
    : SQL.UpdateExpr =
    let entity = layout.FindEntity fieldRef.Entity |> Option.get
    let fixedExpr = replaceEntityRefInExpr (Some <| relaxEntityRef entity.Root) expr
    let result = queryColumnResult fixedExpr
    let myFromEntity = fromEntity <| relaxEntityRef entity.Root

    let singleSelect =
        { emptySingleSelectExpr with
            Results = [| QRExpr result |]
            From = Some(FEntity myFromEntity) }

    let select = selectExpr (SSelect singleSelect)

    let (compiledResult, compiledFrom) =
        match compileSelectExpr layout emptyArguments select with
        | (_,
           { Tree = SQL.SSelect { Columns = [| SQL.SCExpr(alias, result) |]
                                  From = Some from } }) -> (result, from)
        | _ -> failwith "Unexpected compilation result"

    let tableRef = compileResolvedEntityRef entity.Root

    let updateId =
        { Table = Some tableRef
          Name = sqlFunId }
        : SQL.ColumnRef

    let joinedUpdateId =
        { Table = Some <| compileRenamedResolvedEntityRef fieldRef.Entity
          Name = sqlFunId }
        : SQL.ColumnRef

    let joinSame =
        SQL.VEBinaryOp(SQL.VEColumn updateId, SQL.BOEq, SQL.VEColumn joinedUpdateId)

    let opTable = SQL.operationTable tableRef

    let assign =
        SQL.UAESet(SQL.updateColumnName comp.ColumnName, SQL.IVExpr compiledResult)

    { SQL.updateExpr opTable with
        Assignments = [| assign |]
        From = Some compiledFrom
        Where = Some joinSame }

let private compileMaterializedFieldBulkStore
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (expr: ResolvedFieldExpr)
    : SQL.UpdateExpr =
    let entity = layout.FindEntity fieldRef.Entity |> Option.get

    let comp =
        match entity.FindField fieldRef.Name with
        | Some { Field = RComputedField({ IsMaterialized = true; Root = Some _ } as comp)
                 ForceRename = false } -> comp
        | _ -> failwith "Impossible non-computed field"

    compileMaterializedFieldUpdate layout fieldRef comp expr

let buildMaterializedFieldStore
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (expr: ResolvedFieldExpr)
    : SQL.DatabaseMeta =
    let entity = layout.FindEntity fieldRef.Entity |> Option.get

    let comp =
        match entity.FindField fieldRef.Name with
        | Some { Field = RComputedField({ IsMaterialized = true; Root = Some _ } as comp)
                 ForceRename = false } -> comp
        | _ -> failwith "Impossible non-computed field"

    let builder = AffectedByExprBuilder(layout, fieldRef.Entity)
    let triggers = builder.FindAffectedByExpr expr

    let compiled = compileMaterializedFieldUpdate layout fieldRef comp expr
    let outerFields = findOuterFields layout expr

    // First pair of triggers: outer, e.g., check when the row itself is updated.
    let outerMeta =
        buildOuterMaterializedFieldStore layout fieldRef comp outerFields compiled
    // Next triggers -- for each dependent field we need to check that a change doesn't violate the constraint.
    triggers
    |> Map.toSeq
    |> Seq.map (fun (key, trig) -> buildInnerMaterializedFieldStore layout fieldRef comp compiled key trig)
    |> Seq.fold SQL.unionDatabaseMeta outerMeta

let buildAssertionsMeta (layout: Layout) (asserts: LayoutAssertions) : SQL.DatabaseMeta =
    let mapFieldOfType fieldOfType =
        buildReferenceOfTypeAssertion layout fieldOfType.FromField fieldOfType.ToEntity

    let colOfTypes =
        asserts.ReferenceOfTypeAssertions |> Set.toSeq |> Seq.map mapFieldOfType

    let mapCheck (key, expr) =
        buildCheckConstraintAssertion layout key expr

    let checkConstrs = asserts.CheckConstraints |> Map.toSeq |> Seq.map mapCheck

    let mapMaterialized (fieldRef, expr) =
        buildMaterializedFieldStore layout fieldRef expr

    let matStores = asserts.MaterializedFields |> Map.toSeq |> Seq.map mapMaterialized

    Seq.concat [ colOfTypes; checkConstrs; matStores ]
    |> Seq.fold SQL.unionDatabaseMeta SQL.emptyDatabaseMeta

let private compileReferenceOfTypeCheck
    (layout: Layout)
    (fromFieldRef: ResolvedFieldRef)
    (toEntityRef: ResolvedEntityRef)
    : SQL.SelectExpr option =
    let joinName = SQL.SQLName "__joined"
    let joinAlias = { Name = joinName; Columns = None }: SQL.TableAlias
    let joinRef = { Schema = None; Name = joinName }: SQL.TableRef

    let subEntityColumn =
        SQL.VEColumn
            { Table = Some joinRef
              Name = sqlFunSubEntity }

    match makeCheckExpr subEntityColumn layout toEntityRef with
    | None -> None
    | Some checkExpr ->
        let entity = layout.FindEntity fromFieldRef.Entity |> Option.get
        let field = Map.find fromFieldRef.Name entity.ColumnFields
        let refEntity = layout.FindEntity toEntityRef |> Option.get

        let fromRef = compileResolvedEntityRef entity.Root
        let toRef = compileResolvedEntityRef refEntity.Root

        let fromColumn =
            SQL.VEColumn
                { Table = Some fromRef
                  Name = field.ColumnName }

        let toColumn =
            SQL.VEColumn
                { Table = Some joinRef
                  Name = sqlFunId }

        let joinExpr = SQL.VEBinaryOp(fromColumn, SQL.BOEq, toColumn)
        let fTableA = SQL.fromTable fromRef

        let fTableB =
            { SQL.fromTable toRef with
                Alias = Some joinAlias }

        let join =
            SQL.FJoin
                { Type = SQL.JoinType.Left
                  A = SQL.FTable fTableA
                  B = SQL.FTable fTableB
                  Condition = joinExpr }

        let singleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = [| SQL.SCExpr(None, SQL.VEAggFunc(SQL.SQLName "bool_and", SQL.AEAll [| checkExpr |])) |]
                From = Some join }

        Some <| SQL.selectExpr (SQL.SSelect singleSelect)

let private runIntegrityCheck
    (conn: QueryConnection)
    (query: SQL.SelectExpr)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        try
            match! conn.ExecuteValueQuery (query.ToSQLString()) Map.empty cancellationToken with
            | None -> ()
            | Some(name, typ, SQL.VNull) -> ()
            | Some(name, typ, SQL.VBool true) -> ()
            | Some(name, typ, SQL.VBool false) -> raisef LayoutIntegrityException "Failed an integrity check"
            | Some(name, typ, ret) -> failwithf "Unexpected result of an integrity check: %O" ret
        with :? QueryExecutionException as e ->
            raisefUserWithInner LayoutIntegrityException e "Query exception"
    }

let checkAssertions
    (conn: QueryConnection)
    (layout: Layout)
    (assertions: LayoutAssertions)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        for columnOfType in assertions.ReferenceOfTypeAssertions do
            match compileReferenceOfTypeCheck layout columnOfType.FromField columnOfType.ToEntity with
            | None -> ()
            | Some query ->
                try
                    do! runIntegrityCheck conn query cancellationToken
                with e ->
                    raisefWithInner
                        LayoutIntegrityException
                        e
                        "Failed to check that all %O values point to %O entries"
                        columnOfType.FromField
                        columnOfType.ToEntity

        for KeyValue(constrRef, expr) in assertions.CheckConstraints do
            let query = compileCheckConstraintBulkCheck layout constrRef expr

            try
                do! runIntegrityCheck conn query cancellationToken
            with e ->
                raisefWithInner LayoutIntegrityException e "Failed to validate check constraint %O" constrRef

        for KeyValue(fieldRef, expr) in assertions.MaterializedFields do
            let query = compileMaterializedFieldBulkStore layout fieldRef expr
            let! _ = conn.ExecuteNonQuery (query.ToSQLString()) Map.empty cancellationToken
            ()
    }
