module FunWithFlags.FunDB.Layout.Meta

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL

type private MetaBuilder (layout : Layout) =
    let rec compileComputedExpr (entity : ResolvedEntity) : ResolvedFieldExpr -> SQL.ValueExpr =
        let makeFullName (ctx : ReferenceContext) : LinkedBoundFieldRef -> SQL.ValueExpr = function
            | { ref = VRColumn { ref = { entity = None; name = name }}; path = [||] } -> compileComputedName entity ctx name
            | c -> failwith <| sprintf "Unexpected reference in computed field expression: %O" c
        let voidQuery _ = failwith <| sprintf "Unexpected query in computed field expression"
        genericCompileFieldExpr layout makeFullName voidQuery

    and compileComputedName (entity : ResolvedEntity) (ctx : ReferenceContext) (name : FieldName) : SQL.ValueExpr =
        let (realName, field) = entity.FindField name |> Option.get
        match field with
        | RId
        | RColumnField _ -> SQL.VEColumn { table = None; name = compileName realName }
        | RSubEntity ->
            match ctx with
            | RCExpr -> entity.subEntityParseExpr
            | RCTypeExpr -> SQL.VEColumn { table = None; name = compileName realName }
        | RComputedField comp -> compileComputedExpr entity comp.expression

    let compileCheckExpr (entity : ResolvedEntity) : LocalFieldExpr -> SQL.ValueExpr =
        let voidQuery _ = failwith <| sprintf "Unexpected query in computed field expression"
        genericCompileFieldExpr layout (compileComputedName entity) voidQuery

    let makeUniqueConstraintMeta (constr : ResolvedUniqueConstraint) : SQL.ConstraintMeta =
        SQL.CMUnique <| Array.map (fun name -> SQL.SQLName <| name.ToString()) constr.columns

    let makeCheckConstraintMeta (entity : ResolvedEntity) (constr : ResolvedCheckConstraint) : SQL.ConstraintMeta =
        SQL.CMCheck <| compileCheckExpr entity constr.expression

    let makeColumnFieldMeta (columnName : SQL.ResolvedColumnRef) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.ConstraintName * SQL.ConstraintMeta) seq =
        let makeDefaultValue def =
            match compileFieldValue def with
            | SQL.VEValue SQL.VNull -> None
            | ret -> Some ret
        let res =
            { columnType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.fieldType)
              isNullable = field.isNullable
              defaultExpr = Option.bind makeDefaultValue field.defaultValue
            } : SQL.ColumnMeta
        let constr =
            match field.fieldType with
                | FTReference (entityRef, restriction) ->
                    // FIXME: support restrictions!
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.root
                    Seq.singleton (SQL.SQLName <| sprintf "__%O_foreign_%O" columnName.table.name columnName.name, SQL.CMForeignKey (tableRef, [| (columnName.name, sqlFunId) |]))
                | FTEnum vals ->
                    let expr =
                        let col = SQL.VEColumn { table = None; name = columnName.name }
                        let makeValue value = SQL.VEValue (SQL.VString value)
                        let exprs = vals |> Set.toSeq |> Seq.map makeValue |> Seq.toArray
                        if Array.isEmpty exprs then
                            if field.isNullable then
                                SQL.VEIsNull col
                            else
                                SQL.VEValue (SQL.VBool false)
                        else if Array.length exprs = 1 then
                            SQL.VEEq (col, exprs.[0])
                        else
                            SQL.VEIn (col, exprs)
                    Seq.singleton (SQL.SQLName <| sprintf "__%O_enum_%O" columnName.table.name columnName.name, SQL.CMCheck expr)
                | _ -> Seq.empty
        (res, constr)

    let makeEntityMeta (ref : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableMeta * (SQL.SQLName * SQL.ObjectMeta) seq =
        let makeUniqueConstraint (name, constr) = (SQL.SQLName <| sprintf "__%O_unique_%O" ref.name name, makeUniqueConstraintMeta constr)
        let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
        let tableName = compileResolvedEntityRef entity.root

        match entity.inheritance with
        | None ->
            let idSeq = SQL.SQLName <| sprintf "__%O_seq_id" ref.name
            let idConstraints =
                let name = SQL.SQLName <| sprintf "__%O_primary_id" ref.name
                let constr = SQL.CMPrimaryKey [| sqlFunId |]
                Seq.singleton (name, constr)

            let idColumns =
                let col =
                    { columnType = SQL.VTScalar (SQL.STInt.ToSQLRawString())
                      isNullable = false
                      defaultExpr = Some <| SQL.VEFunc (SQL.SQLName "nextval", [| SQL.VEValue (SQL.VRegclass { schema = tableName.schema; name = idSeq }) |])
                    } : SQL.ColumnMeta
                Seq.singleton (SQL.SQLName (funId.ToString()), col)
            let (subEntityColumns, subEntityConstraints, subEntityIndexes) =
                if not <| hasSubType entity then
                    (Seq.empty, Seq.empty, Seq.empty)
                else
                    let col =
                        { columnType = SQL.VTScalar (SQL.STString.ToSQLRawString())
                          isNullable = false
                          defaultExpr = if entity.isAbstract then None else Some (SQL.VEValue <| SQL.VString entity.typeName)
                        } : SQL.ColumnMeta
                    let columns = Seq.singleton (SQL.SQLName (funSubEntity.ToString()), col)

                    let checkExpr = makeCheckExpr (layout.FindEntity >> Option.get) entity

                    let constrs = Seq.singleton (SQL.SQLName <| sprintf "__%O_sub_entity_valid" ref.name, SQL.CMCheck checkExpr)
                    let indexes = Seq.singleton (SQL.SQLName <| sprintf "__%O_sub_entity_index" ref.name, ({ columns = [| sqlFunSubEntity |] } : SQL.IndexMeta))

                    (columns, constrs, indexes)

            let makeColumn (name, field) =
                (field.columnName, makeColumnFieldMeta { table = tableName; name = field.columnName } field)
            let makeCheckConstraint (name, constr) = (SQL.SQLName <| sprintf "__%O_check_%O" ref.name name, makeCheckConstraintMeta entity constr)

            let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name, column))
            let columnConstraints = columnObjects |> Seq.collect (fun (name, (column, constrs)) -> constrs)
            let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

            let table = { columns = Seq.concat [idColumns; subEntityColumns; userColumns] |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [idConstraints; subEntityConstraints; uniqueConstraints; checkConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (name, constr) -> (name, SQL.OMConstraint (tableName.name, constr))) constraints
            let indexes = subEntityIndexes
            let indexObjects = Seq.map (fun (name, index) -> (name, SQL.OMIndex (tableName.name, index))) indexes
            let idObject = Seq.singleton (idSeq, SQL.OMSequence)
            let objects = Seq.concat  [idObject; constraintObjects; indexObjects]
            (table, objects)
        | Some inheritance ->
            let makeColumn (name, field : ResolvedColumnField) =
                if Option.isSome field.inheritedFrom then
                    None
                else
                    let (meta, constrs) = makeColumnFieldMeta { table = tableName; name = field.columnName } field
                    let extraConstrs =
                        if meta.isNullable then
                            Seq.empty
                        else
                            let checkNull = SQL.VEIsNull (SQL.VEColumn { table = None; name = field.columnName })
                            let expr = SQL.VENot (SQL.VEAnd (inheritance.checkExpr, checkNull))
                            Seq.singleton (SQL.SQLName <| sprintf "__%O_not_null_%O" ref.name name, SQL.CMCheck expr)
                    Some (field.columnName, ({ meta with isNullable = true }, Seq.append constrs extraConstrs))
            let makeUniqueConstraint (name, constr) = (SQL.SQLName <| sprintf "__%O_unique_%O" ref.name name, makeUniqueConstraintMeta constr)
            let makeCheckConstraint (name, constr) =
                (SQL.SQLName <| sprintf "__%O_check_%O" ref.name name, makeCheckConstraintMeta entity constr)

            let columnObjects = entity.columnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name, column))
            let columnConstraints = columnObjects |> Seq.collect (fun (name, (column, constrs)) -> constrs)
            let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

            let table = { columns = userColumns |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [uniqueConstraints; checkConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (name, constr) -> (name, SQL.OMConstraint (tableName.name, constr))) constraints
            let objects = constraintObjects
            (table, objects)

    let makeSchemaMeta (schemaName : SchemaName) (schema : ResolvedSchema) : Map<ResolvedEntityRef, SQL.TableMeta> * SQL.SchemaMeta =
        let makeEntity (entityName, entity : ResolvedEntity) =
            let ref = { schema = schemaName; name = entityName }
            (entity.root, makeEntityMeta ref entity)
        let ret = schema.entities |> Map.toSeq |> Seq.map makeEntity |> Seq.cache
        let tables = ret |> Seq.map (fun (root, (table, objects)) -> (root, table)) |> Map.ofSeqWith (fun name -> SQL.unionTableMeta)
        let objects = ret |> Seq.collect (fun (root, (table, objects)) -> objects) |> Map.ofSeq
        (tables, { objects = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (compileName ref.schema, Map.singleton (compileName ref.name) (SQL.OMTable table))

        let ret = subLayout.schemas |> Map.toSeq |> Seq.map makeSchema |> Seq.cache
        // Only non-table objects; we need to merge tables.
        let schemas = ret |> Seq.map (fun (name, (tables, objects)) -> (name, objects)) |> Map.ofSeq
        assert Set.isSubset (Map.keysSet schemas) (subLayout.schemas |> Map.keys |> Seq.map compileName |> Set.ofSeq)
        // Tables separately.
        let tables = ret |> Seq.map (fun (name, (tables, objects)) -> tables) |> Seq.fold (Map.unionWith (fun name -> SQL.unionTableMeta)) Map.empty
        let splitTables = tables |> Map.toSeq |> Seq.map makeSplitTable |> Map.ofSeqWith (fun name -> Map.union)
        let mergeTables schemaName (schema : SQL.SchemaMeta) =
            match Map.tryFind schemaName splitTables with
            | None -> schema
            | Some extras -> { schema with objects = Map.union schema.objects extras }
        let allSchemas = Map.map mergeTables schemas

        { SQL.schemas = allSchemas }

    member this.BuildLayoutMeta = buildLayoutMeta

// Sub-layout allows one to build meta only for a part of
let buildLayoutMeta (layout : Layout) (subLayout : Layout) : SQL.DatabaseMeta =
    let builder = MetaBuilder layout
    builder.BuildLayoutMeta subLayout
