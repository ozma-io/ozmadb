module FunWithFlags.FunDB.Layout.Meta

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Typecheck
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL

// Used to create indexes for 
type private PathReferencesMap =  Map<ResolvedFieldRef, ResolvedEntity * ResolvedColumnField>

let private subEntityColumn = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

let private relatedCompilationFlags =
    { emptyExprCompilationFlags with
        ForceNoTableRef = true
    }

let private funExtensions =
    Set.ofSeq
        [ SQL.SQLName "pg_trgm"
        ]

let private defaultIndexColumn (colName : SQL.SQLName) : SQL.IndexColumn =
    { Key = SQL.IKColumn colName
      OpClass = None
      Order = Some SQL.Asc
      Nulls = Some SQL.NullsLast
    }

let private simplifyIndex : SQL.ValueExpr -> SQL.IndexKey = function
    | SQL.VEColumn col -> SQL.IKColumn col.Name
    | expr -> SQL.IKExpression (String.comparable expr)

let private defaultIndexType = SQL.SQLName "btree"

type private MetaBuilder (layout : Layout) =
    let compileRelatedExpr (expr : ResolvedFieldExpr) : SQL.ValueExpr =
        let (arguments, ret) = compileSingleFieldExpr layout relatedCompilationFlags emptyArguments expr
        ret

    let makeUniqueConstraintMeta (entity : ResolvedEntity) (constr : ResolvedUniqueConstraint) : SQL.ConstraintMeta =
        let compileColumn name =
            let col = Map.find name entity.ColumnFields
            col.ColumnName
        SQL.CMUnique (Array.map compileColumn constr.Columns, SQL.DCNotDeferrable)

    let makeIndexColumnMeta (entity : ResolvedEntity) (index : ResolvedIndex) (col : ResolvedIndexColumn) : SQL.IndexColumn =
        let key = compileRelatedExpr col.Expr |> simplifyIndex
        let opClass = col.OpClass |> Option.map (fun opClass -> Map.find opClass allowedOpClasses |> Map.find index.Type)
        
        { Key = key
          OpClass = opClass
          Order = Option.map compileOrder col.Order
          Nulls = Option.map compileNullsOrder col.Nulls
        }

    let makeIndexMeta (ref : ResolvedEntityRef) (entity : ResolvedEntity) (index : ResolvedIndex) : SQL.IndexMeta =
        let predicate = Option.map compileRelatedExpr index.Predicate
        let possibleEntities = allPossibleEntities layout ref |> Seq.map snd |> Array.ofSeq
        let predicate =
            match entity.Parent with
            | None -> predicate
            | Some parent ->
                let check = makeCheckExprFor subEntityColumn possibleEntities
                Option.unionWith (curry SQL.VEAnd) (Some check) predicate

        let columns = Array.map (makeIndexColumnMeta entity index) index.Expressions
        let includedColumns = Array.map (compileRelatedExpr >> simplifyIndex) index.IncludedExpressions

        { Columns = columns
          IncludedColumns = includedColumns
          IsUnique = index.IsUnique
          Predicate = Option.map String.comparable predicate
          AccessMethod = SQL.SQLName <| index.Type.ToFunQLString()
        } : SQL.IndexMeta

    let makeColumnFieldMeta (ref : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.MigrationKey * (SQL.ConstraintName * SQL.ConstraintMeta)) seq =
        let makeDefaultValue def =
            match compileFieldValue def with
            | SQL.VNull -> None
            | ret -> Some <| SQL.VEValue ret
        let res =
            { Name = field.ColumnName
              DataType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.FieldType)
              IsNullable = field.IsNullable
              ColumnType = SQL.CTPlain { DefaultExpr = Option.bind makeDefaultValue field.DefaultValue }
            } : SQL.ColumnMeta
        let constr =
            match field.FieldType with
                | FTReference entityRef ->
                    // FIXME: support restrictions!
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.Root
                    let constrKey = sprintf "__foreign__%s__%s"  entity.HashName field.HashName
                    let constrName = SQL.SQLName constrKey
                    Seq.singleton (constrKey, (constrName, SQL.CMForeignKey (tableRef, [| (field.ColumnName, sqlFunId) |], SQL.DCDeferrable false)))
                | FTEnum vals ->
                    let expr =
                        let col = SQL.VEColumn { Table = None; Name = field.ColumnName }
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
                    Seq.singleton (constrKey, (constrName, SQL.CMCheck (String.comparable expr)))
                | _ -> Seq.empty
        (res, constr)

    let makeMaterializedComputedFieldMeta (ref : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedComputedField) : SQL.ColumnMeta =
        let rootInfo = Option.get field.Root
        let columnType =
            if rootInfo.IsLocal then
                SQL.CTGeneratedStored (makeSingleFieldExpr ref.Entity { Entity = None; Name = ref.Name } |> compileRelatedExpr)
            else
                SQL.CTPlain { DefaultExpr = None }

        { Name = field.ColumnName
          DataType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType rootInfo.Type)
          IsNullable = true
          ColumnType = columnType
        } : SQL.ColumnMeta

    let rec getPathReferences (fieldRef : ResolvedFieldRef) (relatedFields : PathReferencesMap) (fields : (ResolvedEntityRef * FieldName) list) : PathReferencesMap =
        match fields with
        | [] -> relatedFields
        | ((refEntityRef, refName) :: refs) ->
            let entity = layout.FindEntity fieldRef.Entity |> Option.get
            let (fieldName, field) =
                match entity.FindField fieldRef.Name with
                | Some { Field = RColumnField col; Name = fieldName } -> (fieldName, col)
                | _ -> failwith "Unexpected non-column field"
            let relatedFields = Map.add { fieldRef with Name = fieldName } (entity, field) relatedFields
            getPathReferences { Entity = refEntityRef; Name = refName } relatedFields refs

    let makeCheckConstraintMeta (entity : ResolvedEntity) (modifyExpr : SQL.ValueExpr -> SQL.ValueExpr) (constr : ResolvedCheckConstraint) : (SQL.MigrationKey * (SQL.SQLName * SQL.ObjectMeta)) seq =
        let tableName = compileResolvedEntityRef entity.Root
        if not constr.IsLocal then
            Seq.empty
        else
            let expr = modifyExpr <| compileRelatedExpr constr.Expression
            let meta = SQL.CMCheck (String.comparable expr)
            let sqlKey = sprintf "__check__%s__%s" entity.HashName constr.HashName
            let sqlName = SQL.SQLName sqlKey
            Seq.singleton (sqlKey, (sqlName, SQL.OMConstraint (tableName.Name, meta)))

    let makeEntityMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableMeta * (SQL.MigrationKey * (SQL.SQLName * SQL.ObjectMeta)) seq =
        let tableName = compileResolvedEntityRef entity.Root

        let makeEntityCheckConstraint modifyExpr (name, constr) = makeCheckConstraintMeta entity modifyExpr constr

        let makeMaterializedComputedField (name, maybeField) =
            match maybeField with
            | Ok field ->
                if not field.IsMaterialized || Option.isNone field.Root then
                    None
                else
                    let fieldRef = { Entity = entityRef; Name = name }
                    Some (string field.ColumnName, makeMaterializedComputedFieldMeta fieldRef entity field)
            | Error _ -> None
        let materializedFieldColumns = entity.ComputedFields |> Map.toSeq |> Seq.mapMaybe makeMaterializedComputedField

        let (table, extraObjects) =
            match entity.Parent with
            | None ->
                let idSeqName = SQL.SQLName <| sprintf "__idseq__%s" entity.HashName
                let idConstraints =
                    // Correlation step for raw meta should name primary constraints in the same way.
                    let key = sprintf "__correlated__primary__%O" entity.Root.Name
                    let name = SQL.SQLName <| sprintf "__primary__%s" entity.HashName
                    let constr = SQL.CMPrimaryKey ([| sqlFunId |], SQL.DCNotDeferrable)
                    Seq.singleton (key, (name, constr))
                let idColumns =
                    let col =
                        { Name = sqlFunId
                          DataType = SQL.VTScalar (SQL.STInt.ToSQLRawString())
                          IsNullable = false
                          ColumnType = SQL.CTPlain { DefaultExpr = Some <| SQL.VEFunc (SQL.SQLName "nextval", [| SQL.VEValue (SQL.VRegclass { Schema = tableName.Schema; Name = idSeqName }) |]) }
                        } : SQL.ColumnMeta
                    Seq.singleton ("__correlated__id", col)
                let (subEntityColumns, subEntityConstraints, subEntityIndexes) =
                    if not <| hasSubType entity then
                        (Seq.empty, Seq.empty, Seq.empty)
                    else
                        let col =
                            { Name = sqlFunSubEntity
                              DataType = SQL.VTScalar (SQL.STString.ToSQLRawString())
                              IsNullable = false
                              ColumnType = SQL.CTPlain { DefaultExpr = if entity.IsAbstract then None else Some (SQL.VEValue <| SQL.VString entity.TypeName) }
                            } : SQL.ColumnMeta
                        let columns = Seq.singleton (string funSubEntity, col)

                        let checkExpr = makeCheckExpr subEntityColumn layout entityRef

                        let typeCheckKey = sprintf "__type_check__%s" entity.HashName
                        let typeCheckName = SQL.SQLName typeCheckKey
                        let constrs = Seq.singleton (typeCheckKey, (typeCheckName, SQL.CMCheck (String.comparable checkExpr)))
                        let typeIndexKey = sprintf "__type_index__%s" entity.HashName
                        let typeIndexName = SQL.SQLName typeIndexKey
                        let subEntityIndex =
                            { Columns = [| defaultIndexColumn sqlFunSubEntity |]
                              IncludedColumns = [||]
                              IsUnique = false
                              Predicate = None
                              AccessMethod = defaultIndexType
                            } : SQL.IndexMeta
                        let indexes = Seq.singleton (typeIndexKey, (typeIndexName, subEntityIndex))

                        (columns, constrs, indexes)

                let makeColumn (name, field : ResolvedColumnField) =
                    let fieldRef = { Entity = entityRef; Name = name }
                    (string field.ColumnName, (field.ColumnName, makeColumnFieldMeta fieldRef entity field))

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (key, (name, (column, constrs))) -> (name.ToString(), column))
                let columnConstraints = columnObjects |> Seq.collect (fun (key, (name, (column, constrs))) -> constrs)

                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint id)

                let table = { Columns = Seq.concat [idColumns; subEntityColumns; userColumns; materializedFieldColumns] |> Map.ofSeq } : SQL.TableMeta

                let constraints = Seq.concat [idConstraints; subEntityConstraints; columnConstraints]
                let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.Name, constr)))) constraints
                let allIndexes = subEntityIndexes
                let indexObjects = Seq.map (fun (key, (name, index)) -> (key, (name, SQL.OMIndex (tableName.Name, index)))) allIndexes
                // Correlation step for raw meta should name primary sequences in the same way.
                let idSeqKey = sprintf "__correlated__idseq__%O" entity.Root.Name
                let idObject = Seq.singleton (idSeqKey, (idSeqName, SQL.OMSequence))
                let objects = Seq.concat [idObject; constraintObjects; indexObjects; checkConstraintObjects]
                (table, objects)
            | Some parent ->
                let checkExpr = makeCheckExpr subEntityColumn layout entityRef

                let makeColumn (name, field : ResolvedColumnField) =
                    if Option.isSome field.InheritedFrom then
                        None
                    else
                        let fieldRef = { Entity = entityRef; Name = name }
                        let (meta, constrs) = makeColumnFieldMeta fieldRef entity field
                        let extraConstrs =
                            // We do not check that values are NULL if row is not of this entity subtype.
                            // This is to optimize insertion and adding of new columns in case of `DefaultExpr` values set.
                            // Say, one adds a new subtype column with `DEFAULT` set -- because we use native PostgreSQL DEFAULT and allow values to be whatever for non-subtypes,
                            // PostgreSQL can instantly initialize the column with default values for all rows, even not of this subtype.
                            if meta.IsNullable then
                                Seq.empty
                            else
                                let checkNull = SQL.VEIsNull (SQL.VEColumn { Table = None; Name = field.ColumnName })
                                let expr = SQL.VENot (SQL.VEAnd (checkExpr, checkNull))
                                let notnullName = SQL.SQLName <| sprintf "__notnull__%s__%s" entity.HashName field.HashName
                                let notnullKey = sprintf "__notnull__%s__%s"entity.HashName field.HashName
                                Seq.singleton (notnullKey, (notnullName, SQL.CMCheck (String.comparable expr)))
                        Some (string field.ColumnName, ({ meta with IsNullable = true }, Seq.append constrs extraConstrs))

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name, column))
                let columnConstraints = columnObjects |> Seq.collect (fun (name, (column, constrs)) -> constrs)

                let modify expr = SQL.VEOr (SQL.VENot checkExpr, expr)
                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint modify)

                let table = { Columns = Seq.concat [userColumns; materializedFieldColumns] |> Map.ofSeq } : SQL.TableMeta

                let constraints = columnConstraints
                let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.Name, constr)))) constraints
                let objects = Seq.concat [constraintObjects; checkConstraintObjects]
                (table, objects)

        let makeUniqueConstraint (name, constr : ResolvedUniqueConstraint) =
            let key = sprintf "__unique__%s__%s" entity.HashName constr.HashName
            let sqlName = SQL.SQLName key
            (key, (sqlName, makeUniqueConstraintMeta entity constr))
        let uniqueConstraints = entity.UniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
        let commonConstraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.Name, constr)))) uniqueConstraints

        let makeIndex (name, index : ResolvedIndex) =
            let key = sprintf "__index__%s__%s" entity.HashName index.HashName
            let sqlName = SQL.SQLName key
            (key, (sqlName, makeIndexMeta entityRef entity index))
        let indexes = entity.Indexes |> Map.toSeq |> Seq.map makeIndex

        let makeReferenceIndex (name, field : ResolvedColumnField) =
            match field with
            | { InheritedFrom = None; FieldType = FTReference refEntityRef } ->
                // We build indexes for all references to speed up non-local check constraint integrity lookups,
                // and DELETE operations on related fields (PostgreSQL will make use of this index internally, that's why we don't set a predicate).
                let key = sprintf "__refindex__%s__%s" entity.HashName field.HashName
                let sqlName = SQL.SQLName key
                let refIndex =
                    { Columns = [| defaultIndexColumn field.ColumnName |]
                      IncludedColumns = [||]
                      IsUnique = false
                      Predicate = None
                      AccessMethod = defaultIndexType
                    } : SQL.IndexMeta
                Some (key, (sqlName, refIndex))
            | _ -> None
        let refIndexes = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeReferenceIndex

        let allCommonIndexes = Seq.concat [indexes; refIndexes]
        let commonIndexObjects = Seq.map (fun (key, (name, index)) -> (key, (name, SQL.OMIndex (tableName.Name, index)))) allCommonIndexes

        let commonObjects = Seq.concat [commonConstraintObjects; commonIndexObjects]

        (table, Seq.append commonObjects extraObjects)

    let makeSchemaMeta (schemaName : SchemaName) (schema : ResolvedSchema) : Map<ResolvedEntityRef, SQL.TableMeta> * SQL.SchemaMeta =
        let makeEntity (entityName, entity : ResolvedEntity) =
            let ref = { Schema = schemaName; Name = entityName }
            (entity.Root, makeEntityMeta ref entity)
        let ret = schema.Entities |> Map.toSeq |> Seq.map makeEntity |> Seq.cache
        let tables = ret |> Seq.map (fun (root, (table, objects)) -> (root, table)) |> Map.ofSeqWith (fun name -> SQL.unionTableMeta)
        let objects = ret |> Seq.collect (fun (root, (table, objects)) -> objects) |> Map.ofSeq
        (tables, { Name = compileName schemaName; Objects = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (ref.Schema.ToString(), Map.singleton (ref.Name.ToString()) (compileName ref.Name, SQL.OMTable table))

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

        { Schemas = schemas
          Extensions = funExtensions
        }

    member this.BuildLayoutMeta = buildLayoutMeta

// Sub-layout allows one to build meta only for a part of
let buildLayoutMeta (layout : Layout) (subLayout : Layout) : SQL.DatabaseMeta =
    let builder = MetaBuilder (layout)
    builder.BuildLayoutMeta subLayout
