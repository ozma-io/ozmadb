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
    let rec compileRelatedExpr (entity : ResolvedEntity) : ResolvedFieldExpr -> SQL.ValueExpr =
        let makeFullName (ctx : ReferenceContext) : LinkedBoundFieldRef -> SQL.ValueExpr = function
            | { Ref = VRColumn { Ref = { entity = None; name = name }}; Path = [||] } -> compileComputedName entity ctx name
            | c -> failwith <| sprintf "Unexpected reference in computed field expression: %O" c
        let voidQuery _ = failwith <| sprintf "Unexpected query in computed field expression"
        genericCompileFieldExpr layout makeFullName voidQuery

    and compileComputedName (entity : ResolvedEntity) (ctx : ReferenceContext) (name : FieldName) : SQL.ValueExpr =
        let fieldInfo = entity.FindField name |> Option.get
        match fieldInfo.Field with
        | RId
        | RColumnField _ -> SQL.VEColumn { table = None; name = compileName fieldInfo.Name }
        | RSubEntity ->
            match ctx with
            | RCExpr -> entity.SubEntityParseExpr
            | RCTypeExpr -> SQL.VEColumn { table = None; name = compileName fieldInfo.Name }
        | RComputedField comp -> compileRelatedExpr entity comp.Expression

    let compileLocalExpr (entity : ResolvedEntity) : LocalFieldExpr -> SQL.ValueExpr =
        let voidQuery _ = failwith <| sprintf "Unexpected query in local expression"
        genericCompileFieldExpr layout (compileComputedName entity) voidQuery

    let makeUniqueConstraintMeta (entity : ResolvedEntity) (constr : ResolvedUniqueConstraint) : SQL.ConstraintMeta =
        let compileColumn name =
            let col = Map.find name entity.ColumnFields
            col.ColumnName
        SQL.CMUnique (Array.map compileColumn constr.Columns, SQL.defaultDeferrableConstraint)

    let makeIndexMeta (entity : ResolvedEntity) (index : ResolvedIndex) : SQL.IndexMeta =
        let compileExpr expr = compileLocalExpr entity expr |> String.comparable |> SQL.IKExpression
        let compileKey : LocalFieldExpr -> SQL.IndexKey = function
            | FERef r as expr ->
                match Map.tryFind r entity.ColumnFields with
                | Some col -> SQL.IKColumn col.ColumnName
                | None -> compileExpr expr
            | expr -> compileExpr expr

        { Keys = Array.map compileKey index.Expressions
          IsUnique = index.IsUnique
        } : SQL.IndexMeta

    let makeColumnFieldMeta (ref : ResolvedFieldRef) (columnName : SQL.ColumnName) (entity : ResolvedEntity) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.MigrationKey * (SQL.ConstraintName * SQL.ConstraintMeta)) seq =
        let makeDefaultValue def =
            match compileFieldValue def with
            | SQL.VNull -> None
            | ret -> Some <| SQL.VEValue ret
        let res =
            { Name = columnName
              ColumnType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.FieldType)
              IsNullable = field.IsNullable
              DefaultExpr = Option.bind makeDefaultValue field.DefaultValue
            } : SQL.ColumnMeta
        let constr =
            match field.FieldType with
                | FTReference entityRef ->
                    // FIXME: support restrictions!
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.Root
                    let constrKey = sprintf "__foreign__%s__%s"  entity.HashName field.HashName
                    let constrName = SQL.SQLName constrKey
                    Seq.singleton (constrKey, (constrName, SQL.CMForeignKey (tableRef, [| (columnName, sqlFunId) |], deferrableConstraint)))
                | FTEnum vals ->
                    let expr =
                        let col = SQL.VEColumn { table = None; name = columnName }
                        let makeValue value = SQL.VEValue (SQL.VString value)
                        let exprs = vals |> Set.toSeq |> Seq.map makeValue |> Seq.toArray
                        if Array.isEmpty exprs then
                            if field.IsNullable then
                                SQL.VEIsNull col
                            else
                                SQL.VEValue (SQL.VBool false)
                        else if Array.length exprs = 1 then
                            SQL.VEBinaryOp (col, SQL.BOEq, exprs.[0])
                        else
                            SQL.VEIn (col, exprs)
                    let constrKey = sprintf "__enum__%s__%s" entity.HashName field.HashName
                    let constrName = SQL.SQLName constrKey
                    Seq.singleton (constrKey, (constrName, SQL.CMCheck expr))
                | _ -> Seq.empty
        (res, constr)

    let rec getPathReferences (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (fields : FieldName list) : Map<ResolvedFieldRef, ResolvedEntity * ResolvedColumnField> =
        match fields with
        | [] -> Map.empty
        | (ref :: refs) ->
            match Map.tryFind fieldRef.name entity.ColumnFields with
            | Some ({ FieldType = FTReference refEntity } as field) ->
                let newEntity = layout.FindEntity refEntity |> Option.get
                let ret = getPathReferences newEntity { entity = refEntity; name = ref } refs
                Map.add fieldRef (entity, field) ret
            | _ -> failwithf "Invalid dereference in path: %O" ref

    let makeCheckConstraintMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (modifyExpr : SQL.ValueExpr -> SQL.ValueExpr) (constr : ResolvedCheckConstraint) : (SQL.MigrationKey * (SQL.SQLName * SQL.ObjectMeta)) seq =
        let tableName = compileResolvedEntityRef entity.Root
        if not constr.IsLocal then
            // We generate indexes to make validation queries faster.
            let mutable relatedFields = Map.empty
            let addReference (fieldRef : LinkedBoundFieldRef) =
                let col =
                    match fieldRef.Ref with
                    | VRColumn col -> col
                    | r -> failwith <| sprintf "Unexpected placeholder: %O" r
                let refs = getPathReferences entity { entity = entityRef; name = col.Ref.name } (Array.toList fieldRef.Path)
                relatedFields <- Map.union refs relatedFields
            let mapper = { idFieldExprIter with FieldReference = addReference }
            iterFieldExpr mapper constr.Expression
            let createIndex (fieldRef : ResolvedFieldRef, (refEntity : ResolvedEntity, refField : ResolvedColumnField)) =
                let sqlKey = sprintf "__sysindex__%s__%s" refEntity.HashName refField.HashName
                let sqlName = SQL.SQLName sqlKey
                let index =
                    { Keys = [| SQL.IKColumn refField.ColumnName |]
                      IsUnique = false
                    } : SQL.IndexMeta
                (sqlKey, (sqlName, SQL.OMIndex (compileName refEntity.Root.name, index)))
            relatedFields |> Map.toSeq |> Seq.map createIndex
        else
            let expr = modifyExpr <| compileRelatedExpr entity constr.Expression
            let meta = SQL.CMCheck expr
            let sqlKey = sprintf "__check__%s__%s" entity.HashName constr.HashName
            let sqlName = SQL.SQLName sqlKey
            Seq.singleton (sqlKey, (sqlName, SQL.OMConstraint (tableName.name, meta)))

    let makeEntityMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableMeta * (SQL.MigrationKey * (SQL.SQLName * SQL.ObjectMeta)) seq =
        let makeUniqueConstraint (name, constr : ResolvedUniqueConstraint) =
            let key = sprintf "__unique__%s__%s" entity.HashName constr.HashName
            let sqlName = SQL.SQLName key
            (key, (sqlName, makeUniqueConstraintMeta entity constr))
        let uniqueConstraints = entity.UniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
        let makeIndex (name, index : ResolvedIndex) =
            let key = sprintf "__index__%s__%s" entity.HashName index.HashName
            let sqlName = SQL.SQLName key
            (key, (sqlName, makeIndexMeta entity index))
        let indexes = entity.Indexes |> Map.toSeq |> Seq.map makeIndex
        let tableName = compileResolvedEntityRef entity.Root

        let makeEntityCheckConstraint modifyExpr (name, constr) = makeCheckConstraintMeta entityRef entity modifyExpr constr

        match entity.Inheritance with
        | None ->
            let idSeqKey = sprintf "__id__%s" entity.HashName
            let idSeqName = SQL.SQLName idSeqKey
            let idConstraints =
                let key = sprintf "__primary__%s" entity.HashName
                let name = SQL.SQLName key
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
                          DefaultExpr = if entity.IsAbstract then None else Some (SQL.VEValue <| SQL.VString entity.TypeName)
                        } : SQL.ColumnMeta
                    let columns = Seq.singleton (funSubEntity.ToString(), col)

                    let checkExpr = makeCheckExpr (layout.FindEntity >> Option.get) entity

                    let typeCheckKey = sprintf "__type_check__%s" entity.HashName
                    let typeCheckName = SQL.SQLName typeCheckKey
                    let constrs = Seq.singleton (typeCheckKey, (typeCheckName, SQL.CMCheck checkExpr))
                    let typeIndexKey = sprintf "__type_index__%s" entity.HashName
                    let typeIndexName = SQL.SQLName typeIndexKey
                    let subEntityIndex =
                        { Keys = [| SQL.IKColumn sqlFunSubEntity |]
                          IsUnique = false
                        } : SQL.IndexMeta
                    let indexes = Seq.singleton (typeIndexKey, (typeIndexName, subEntityIndex))

                    (columns, constrs, indexes)

            let makeColumn (name, field) =
                let fieldRef = { entity = entityRef; name = name }
                (name.ToString(), (field.ColumnName, makeColumnFieldMeta fieldRef field.ColumnName entity field))

            let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (key, (name, (column, constrs))) -> (name.ToString(), column))
            let columnConstraints = columnObjects |> Seq.collect (fun (key, (name, (column, constrs))) -> constrs)

            let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint id)

            let table = { Columns = Seq.concat [idColumns; subEntityColumns; userColumns] |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [idConstraints; subEntityConstraints; uniqueConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.name, constr)))) constraints
            let allIndexes = Seq.concat [indexes; subEntityIndexes]
            let indexObjects = Seq.map (fun (key, (name, index)) -> (key, (name, SQL.OMIndex (tableName.name, index)))) allIndexes
            let idObject = Seq.singleton (idSeqKey, (idSeqName, SQL.OMSequence))
            let objects = Seq.concat [idObject; constraintObjects; indexObjects; checkConstraintObjects]
            (table, objects)
        | Some inheritance ->
            let makeColumn (name, field : ResolvedColumnField) =
                if Option.isSome field.InheritedFrom then
                    None
                else
                    let fieldRef = { entity = entityRef; name = name }
                    let (meta, constrs) = makeColumnFieldMeta fieldRef field.ColumnName entity field
                    let extraConstrs =
                        if meta.IsNullable then
                            Seq.empty
                        else
                            let checkNull = SQL.VEIsNull (SQL.VEColumn { table = None; name = field.ColumnName })
                            let expr = SQL.VENot (SQL.VEAnd (inheritance.CheckExpr, checkNull))
                            let notnullName = SQL.SQLName <| sprintf "__notnull__%s__%s" entity.HashName field.HashName
                            let notnullKey = sprintf "__notnull__%s__%s"entity.HashName field.HashName
                            Seq.singleton (notnullKey, (notnullName, SQL.CMCheck expr))
                    Some (field.ColumnName, ({ meta with IsNullable = true }, Seq.append constrs extraConstrs))

            let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
            let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name.ToString(), column))
            let columnConstraints = columnObjects |> Seq.collect (fun (name, (column, constrs)) -> constrs)

            let modify expr = SQL.VEOr (SQL.VENot (inheritance.CheckExpr), expr)
            let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint modify)

            let table = { Columns = userColumns |> Map.ofSeq } : SQL.TableMeta

            let constraints = Seq.concat [uniqueConstraints; columnConstraints]
            let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.name, constr)))) constraints
            let objects = Seq.concat [constraintObjects; checkConstraintObjects]
            (table, objects)

    let makeSchemaMeta (schemaName : SchemaName) (schema : ResolvedSchema) : Map<ResolvedEntityRef, SQL.TableMeta> * SQL.SchemaMeta =
        let makeEntity (entityName, entity : ResolvedEntity) =
            let ref = { schema = schemaName; name = entityName }
            (entity.Root, makeEntityMeta ref entity)
        let ret = schema.Entities |> Map.toSeq |> Seq.map makeEntity |> Seq.cache
        let tables = ret |> Seq.map (fun (root, (table, objects)) -> (root, table)) |> Map.ofSeqWith (fun name -> SQL.unionTableMeta)
        let objects = ret |> Seq.collect (fun (root, (table, objects)) -> objects) |> Map.ofSeq
        (tables, { Name = compileName schemaName; Objects = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (ref.schema.ToString(), Map.singleton (ref.name.ToString()) (compileName ref.name, SQL.OMTable table))

        let ret = subLayout.Schemas |> Map.toSeq |> Seq.map makeSchema |> Seq.cache
        // Only non-table objects; we need to merge tables.
        let schemas = ret |> Seq.map (fun (name, (tables, objects)) -> (name.ToString(), objects)) |> Map.ofSeq
        assert Set.isSubset (Map.keysSet schemas) (subLayout.Schemas |> Map.keys |> Seq.map string |> Set.ofSeq)
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