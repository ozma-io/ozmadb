module FunWithFlags.FunDB.Layout.Meta

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Resolve
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL

let private deferrableConstraint =
    { SQL.defaultDeferrableConstraint with
          Deferrable = true
    }

type private MetaBuilder (layout : Layout) =
    let rec compileComputedExpr (entity : ResolvedEntity) : ResolvedFieldExpr -> SQL.ValueExpr =
        let makeFullName (ctx : ReferenceContext) : LinkedBoundFieldRef -> SQL.ValueExpr = function
            | { Ref = VRColumn { Ref = { entity = None; name = name }}; Path = [||] } -> compileComputedName entity ctx name
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

    let makeUniqueConstraintMeta (entity : ResolvedEntity) (constr : ResolvedUniqueConstraint) : SQL.ConstraintMeta =
        let compileColumn name =
            let col = Map.find name entity.columnFields
            col.columnName
        SQL.CMUnique (Array.map compileColumn constr.columns, SQL.defaultDeferrableConstraint)

    let makeColumnFieldMeta (ref : ResolvedFieldRef) (columnName : SQL.ColumnName) (entity : ResolvedEntity) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.MigrationKey * (SQL.ConstraintName * SQL.ConstraintMeta)) seq =
        let makeDefaultValue def =
            match compileFieldValue def with
            | SQL.VEValue SQL.VNull -> None
            | ret -> Some ret
        let res =
            { Name = columnName
              ColumnType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.fieldType)
              IsNullable = field.isNullable
              DefaultExpr = Option.bind makeDefaultValue field.defaultValue
            } : SQL.ColumnMeta
        let constr =
            match field.fieldType with
                | FTReference (entityRef, restriction) ->
                    // FIXME: support restrictions!
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.root
                    let constrName = SQL.SQLName <| sprintf "__foreign__%s__%s"  entity.hashName field.hashName
                    let constrKey = sprintf "__foreign__%s__%s"  entity.hashName field.hashName
                    Seq.singleton (constrKey, (constrName, SQL.CMForeignKey (tableRef, [| (columnName, sqlFunId) |], deferrableConstraint)))
                | FTEnum vals ->
                    let expr =
                        let col = SQL.VEColumn { table = None; name = columnName }
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
                    let constrName = SQL.SQLName <| sprintf "__enum__%s__%s" entity.hashName field.hashName
                    let constrKey = sprintf "__enum__%s__%s" entity.hashName field.hashName
                    Seq.singleton (constrKey, (constrName, SQL.CMCheck expr))
                | _ -> Seq.empty
        (res, constr)

    let makeEntityMeta (ref : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableMeta * (SQL.MigrationKey * (SQL.SQLName * SQL.ObjectMeta)) seq =
        let makeUniqueConstraint (name, constr : ResolvedUniqueConstraint) =
            let name = SQL.SQLName <| sprintf "__unique__%s__%s" entity.hashName constr.hashName
            let key = sprintf "__unique__%s__%s"entity.hashName constr.hashName
            (key, (name, makeUniqueConstraintMeta entity constr))
        let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
        let tableName = compileResolvedEntityRef entity.root

        match entity.inheritance with
        | None ->
            let idSeqName = SQL.SQLName <| sprintf "__id__%s" entity.hashName
            let idSeqKey = sprintf "__id__%s"entity.hashName
            let idConstraints =
                let name = SQL.SQLName <| sprintf "__primary__%s" entity.hashName
                let key = sprintf "__primary__%s"entity.hashName
                let constr = SQL.CMPrimaryKey ([| sqlFunId |], SQL.defaultDeferrableConstraint)
                Seq.singleton (key, (name, constr))

            let idColumns =
                let col =
                    { Name = sqlFunId
                      ColumnType = SQL.VTScalar (SQL.STInt.ToSQLRawString())
                      IsNullable = false
                      DefaultExpr = Some <| SQL.VEFunc (SQL.SQLName "nextval", [| SQL.VEValue (SQL.VRegclass { schema = tableName.schema; name = idSeqName }) |])
                    } : SQL.ColumnMeta
                Seq.singleton (funId.ToString(), col)
            let (subEntityColumns, subEntityConstraints, subEntityIndexes) =
                if not <| hasSubType entity then
                    (Seq.empty, Seq.empty, Seq.empty)
                else
                    let col =
                        { Name = sqlFunSubEntity
                          ColumnType = SQL.VTScalar (SQL.STString.ToSQLRawString())
                          IsNullable = false
                          DefaultExpr = if entity.isAbstract then None else Some (SQL.VEValue <| SQL.VString entity.typeName)
                        } : SQL.ColumnMeta
                    let columns = Seq.singleton (funSubEntity.ToString(), col)

                    let checkExpr = makeCheckExpr (layout.FindEntity >> Option.get) entity

                    let typeCheckName = SQL.SQLName <| sprintf "__type_check__%s" entity.hashName
                    let typeCheckKey = sprintf "__type_check__%s"entity.hashName
                    let constrs = Seq.singleton (typeCheckKey, (typeCheckName, SQL.CMCheck checkExpr))
                    let typeIndexName = SQL.SQLName <| sprintf "__type_index__%s" entity.hashName
                    let typeIndexKey = sprintf "__type_index__%s"entity.hashName
                    let indexes = Seq.singleton (typeIndexKey, (typeIndexName, ({ Columns = [| sqlFunSubEntity |] } : SQL.IndexMeta)))

                    (columns, constrs, indexes)

            let makeColumn (name, field) =
                let fieldRef = { entity = ref; name = name }
                (name.ToString(), (field.columnName, makeColumnFieldMeta fieldRef field.columnName entity field))
            let makeCheckConstraint (name, constr : ResolvedCheckConstraint) =
                let meta = SQL.CMCheck <| compileCheckExpr entity constr.expression
                let sqlName = SQL.SQLName <| sprintf "__check__%s__%s" entity.hashName constr.hashName
                let sqlKey = sprintf "__check__%s__%s"entity.hashName constr.hashName
                (sqlKey, (sqlName, meta))

            let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (key, (name, (column, constrs))) -> (name.ToString(), column))
            let columnConstraints = columnObjects |> Seq.collect (fun (key, (name, (column, constrs))) -> constrs)
            let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

            let table = { Columns = Seq.concat [idColumns; subEntityColumns; userColumns] |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [idConstraints; subEntityConstraints; uniqueConstraints; checkConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.name, constr)))) constraints
            let indexes = subEntityIndexes
            let indexObjects = Seq.map (fun (key, (name, index)) -> (key, (name, SQL.OMIndex (tableName.name, index)))) indexes
            let idObject = Seq.singleton (idSeqKey, (idSeqName, SQL.OMSequence))
            let objects = Seq.concat [idObject; constraintObjects; indexObjects]
            (table, objects)
        | Some inheritance ->
            let makeColumn (name, field : ResolvedColumnField) =
                if Option.isSome field.inheritedFrom then
                    None
                else
                    let fieldRef = { entity = ref; name = name }
                    let (meta, constrs) = makeColumnFieldMeta fieldRef field.columnName entity field
                    let extraConstrs =
                        if meta.IsNullable then
                            Seq.empty
                        else
                            let checkNull = SQL.VEIsNull (SQL.VEColumn { table = None; name = field.columnName })
                            let expr = SQL.VENot (SQL.VEAnd (inheritance.checkExpr, checkNull))
                            let notnullName = SQL.SQLName <| sprintf "__notnull__%s__%s" entity.hashName field.hashName
                            let notnullKey = sprintf "__notnull__%s__%s"entity.hashName field.hashName
                            Seq.singleton (notnullKey, (notnullName, SQL.CMCheck expr))
                    Some (field.columnName, ({ meta with IsNullable = true }, Seq.append constrs extraConstrs))
            let makeCheckConstraint (name, constr : ResolvedCheckConstraint) =
                let expr = compileCheckExpr entity constr.expression
                let check = SQL.VEOr (SQL.VENot (inheritance.checkExpr), expr)
                let meta = SQL.CMCheck check
                let checkName = SQL.SQLName <| sprintf "__check__%s__%s" entity.hashName constr.hashName
                let checkKey = sprintf "__check__%s__%s"entity.hashName constr.hashName
                (checkKey, (checkName, meta))

            let columnObjects = entity.columnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name.ToString(), column))
            let columnConstraints = columnObjects |> Seq.collect (fun (name, (column, constrs)) -> constrs)
            let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

            let table = { Columns = userColumns |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [uniqueConstraints; checkConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.name, constr)))) constraints
            let objects = constraintObjects
            (table, objects)

    let makeSchemaMeta (schemaName : SchemaName) (schema : ResolvedSchema) : Map<ResolvedEntityRef, SQL.TableMeta> * SQL.SchemaMeta =
        let makeEntity (entityName, entity : ResolvedEntity) =
            let ref = { schema = schemaName; name = entityName }
            (entity.root, makeEntityMeta ref entity)
        let ret = schema.entities |> Map.toSeq |> Seq.map makeEntity |> Seq.cache
        let tables = ret |> Seq.map (fun (root, (table, objects)) -> (root, table)) |> Map.ofSeqWith (fun name -> SQL.unionTableMeta)
        let objects = ret |> Seq.collect (fun (root, (table, objects)) -> objects) |> Map.ofSeq
        (tables, { Name = compileName schemaName; Objects = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (ref.schema.ToString(), Map.singleton (ref.name.ToString()) (compileName ref.name, SQL.OMTable table))

        let ret = subLayout.schemas |> Map.toSeq |> Seq.map makeSchema |> Seq.cache
        // Only non-table objects; we need to merge tables.
        let schemas = ret |> Seq.map (fun (name, (tables, objects)) -> (name.ToString(), objects)) |> Map.ofSeq
        assert Set.isSubset (Map.keysSet schemas) (subLayout.schemas |> Map.keys |> Seq.map string |> Set.ofSeq)
        // Tables separately.
        let tables = ret |> Seq.map (fun (name, (tables, objects)) -> tables) |> Seq.fold (Map.unionWith (fun name -> SQL.unionTableMeta)) Map.empty
        let splitTables = tables |> Map.toSeq |> Seq.map makeSplitTable |> Map.ofSeqWith (fun name -> Map.union)
        let mergeTables schemaName (schema : SQL.SchemaMeta) =
            match Map.tryFind schemaName splitTables with
            | None -> schema
            | Some extras -> { schema with Objects = Map.union schema.Objects extras }
        let schemas = Map.map mergeTables schemas

        let schemas = schemas |> Map.filter (fun name schema -> not <| Map.isEmpty schema.Objects)

        { SQL.Schemas = schemas }

    member this.BuildLayoutMeta = buildLayoutMeta

// Sub-layout allows one to build meta only for a part of
let buildLayoutMeta (layout : Layout) (subLayout : Layout) : SQL.DatabaseMeta =
    let builder = MetaBuilder (layout)
    builder.BuildLayoutMeta subLayout