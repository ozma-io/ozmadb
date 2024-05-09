module OzmaDB.Layout.Meta

open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Compile
open OzmaDB.OzmaQL.Arguments
open OzmaDB.Layout.Types
open OzmaDB.OzmaQL.AST
module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.DDL

// Used to create indexes for
type private PathReferencesMap =  Map<ResolvedFieldRef, ResolvedEntity * ResolvedColumnField>

let private subEntityColumn = SQL.VEColumn { Table = None; Name = sqlFunSubEntity }

let private relatedCompilationFlags =
    { emptyExprCompilationFlags with
        ForceNoTableRef = true
    }

let private materializedCompilationFlags =
    { relatedCompilationFlags with
        ForceNoMaterialized = true
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

let primaryConstraintKey = "__primary"

type private MetaBuilder (layout : Layout) =
    let compileRelatedExpr (expr : ResolvedFieldExpr) : SQL.ValueExpr =
        let (arguments, ret) = compileSingleFieldExpr defaultCompilationFlags layout relatedCompilationFlags emptyArguments expr
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
        let predicate =
            match allPossibleEntities layout ref with
            | PEAny -> predicate
            | PEList possibleEntities ->
                let check = makeCheckExprFor subEntityColumn (Seq.map snd possibleEntities)
                Option.unionWith (curry SQL.VEAnd) (Some check) predicate

        let columns = Array.map (makeIndexColumnMeta entity index) index.Expressions
        let includedColumns = Array.map (compileRelatedExpr >> simplifyIndex) index.IncludedExpressions

        { Columns = columns
          IncludedColumns = includedColumns
          IsUnique = index.IsUnique
          Predicate = Option.map String.comparable predicate
          AccessMethod = SQL.SQLName <| index.Type.ToOzmaQLString()
        } : SQL.IndexMeta

    let makeColumnFieldMeta (ref : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.ConstraintName * (SQL.MigrationKeysSet * SQL.ConstraintMeta)) seq =
        let makeDefaultValue def =
            match compileFieldValue def with
            | SQL.VNull -> None
            | ret -> Some <| SQL.VEValue ret
        let res =
            { DataType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.FieldType)
              IsNullable = field.IsNullable
              ColumnType = SQL.CTPlain { DefaultExpr = Option.bind makeDefaultValue field.DefaultValue }
            } : SQL.ColumnMeta
        let constr =
            match field.FieldType with
                | FTScalar (SFTReference (entityRef, opts)) ->
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.Root
                    let constrKey = sprintf "__foreign__%O__%O__%O" ref.Entity.Schema ref.Entity.Name ref.Name
                    let constrName = foreignConstraintSQLName entity.HashName field.HashName
                    let deleteOpt =
                        match opts with
                        | None
                        | Some RDANoAction -> SQL.DANoAction
                        | Some RDACascade -> SQL.DACascade
                        | Some RDASetDefault -> SQL.DASetDefault
                        | Some RDASetNull -> SQL.DASetNull
                    let opts =
                        { Defer = SQL.DCDeferrable false
                          OnDelete = deleteOpt
                          OnUpdate = SQL.DANoAction
                          ToTable = tableRef
                          Columns = [| (field.ColumnName, sqlFunId) |]
                        } : SQL.ForeignKeyMeta
                    Seq.singleton (constrName, (Set.singleton constrKey, SQL.CMForeignKey opts))
                | FTScalar (SFTEnum vals) ->
                    let expr =
                        let col = SQL.VEColumn { Table = None; Name = field.ColumnName }
                        let makeValue value = SQL.VEValue (SQL.VString value)
                        // We sort them here to avoid rebuilding check constraints when order changes.
                        let exprs = vals |> Seq.sort |> Seq.map makeValue |> Seq.toArray
                        if Array.isEmpty exprs then
                            if field.IsNullable then
                                SQL.VEIsNull col
                            else
                                SQL.VEValue (SQL.VBool false)
                        else if Array.length exprs = 1 then
                            SQL.VEBinaryOp (col, SQL.BOEq, exprs.[0])
                        else
                            SQL.VEIn (col, exprs)
                    let constrKey = sprintf "__enum__%O__%O__%O" ref.Entity.Schema ref.Entity.Name ref.Name
                    let constrName = SQL.SQLName <| sprintf "__enum__%s__%s" entity.HashName field.HashName
                    Seq.singleton (constrName, (Set.singleton constrKey, SQL.CMCheck (String.comparable expr)))
                | _ -> Seq.empty
        (res, constr)

    let makeMaterializedComputedFieldMeta (ref : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedComputedField) : SQL.ColumnMeta =
        let rootInfo = Option.get field.Root
        let columnType =
            if exprIsLocal rootInfo.Flags then
                let meta = simpleColumnMeta ref.Entity
                let resolved = makeSingleFieldExpr layout meta { Entity = None; Name = ref.Name }
                let (arguments, compiled) = compileSingleFieldExpr defaultCompilationFlags layout materializedCompilationFlags emptyArguments resolved
                SQL.CTGeneratedStored compiled
            else
                SQL.CTPlain { DefaultExpr = None }

        { DataType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType rootInfo.Type)
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

    let makeCheckConstraintMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (modifyExpr : SQL.ValueExpr -> SQL.ValueExpr) (name : ConstraintName) (constr : ResolvedCheckConstraint) : (SQL.SQLName * (SQL.MigrationKeysSet * SQL.ConstraintMeta)) seq =
        if not constr.IsLocal then
            Seq.empty
        else
            let expr = modifyExpr <| compileRelatedExpr constr.Expression
            let meta = SQL.CMCheck (String.comparable expr)
            let sqlKey = sprintf "__check__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = checkConstraintSQLName entity.HashName constr.HashName
            Seq.singleton (sqlName, (Set.singleton sqlKey, meta))

    let makeEntityMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableObjectsMeta * (SQL.SQLName * SQL.RelationMeta) seq =
        let tableName = compileResolvedEntityRef entity.Root

        let makeEntityCheckConstraint modifyExpr (name, constr) = makeCheckConstraintMeta entityRef entity modifyExpr name constr

        let makeMaterializedComputedField (name, maybeField) =
            match maybeField with
            | Ok field ->
                if not field.IsMaterialized || Option.isNone field.Root || Option.isSome field.InheritedFrom then
                    None
                else
                    let fieldRef = { Entity = entityRef; Name = name }
                    Some (field.ColumnName, (Set.empty, makeMaterializedComputedFieldMeta fieldRef entity field))
            | Error _ -> None
        let materializedFieldColumns = entity.ComputedFields |> Map.toSeq |> Seq.mapMaybe makeMaterializedComputedField

        let (tableObjects, extraObjects) =
            match entity.Parent with
            | None ->
                let idSeqName = SQL.SQLName <| sprintf "__idseq__%s" entity.HashName
                let idConstraints =
                    // We use entity hash name here because primary key, unique and exclusion constraints create underlying indexes,
                    // which use the same name (but reside in the relations namespace).
                    let name = SQL.SQLName <| sprintf "__primary__%s" entity.HashName
                    let constr = SQL.CMPrimaryKey ([| sqlFunId |], SQL.DCNotDeferrable)
                    Seq.singleton (name, (Set.singleton primaryConstraintKey, constr))
                let idColumns =
                    let col =
                        { DataType = SQL.VTScalar (SQL.STInt.ToSQLRawString())
                          IsNullable = false
                          ColumnType = SQL.CTPlain { DefaultExpr = Some <| SQL.VEFunc (SQL.SQLName "nextval", [| SQL.VEValue (SQL.VRegclass { Schema = tableName.Schema; Name = idSeqName }) |]) }
                        } : SQL.ColumnMeta
                    Seq.singleton (sqlFunId, (Set.empty, col))
                let (subEntityColumns, subEntityConstraints, subEntityIndexes) =
                    if not <| hasSubType entity then
                        (Seq.empty, Seq.empty, Seq.empty)
                    else
                        let col =
                            { DataType = SQL.VTScalar (SQL.STString.ToSQLRawString())
                              IsNullable = false
                              ColumnType = SQL.CTPlain { DefaultExpr = if entity.IsAbstract then None else Some (SQL.VEValue <| SQL.VString entity.TypeName) }
                            } : SQL.ColumnMeta
                        let columns = Seq.singleton (sqlFunSubEntity, (Set.empty, col))

                        let constrs =
                            match makeCheckExpr subEntityColumn layout entityRef with
                            | None -> Seq.empty
                            | Some checkExpr ->
                                let typeCheckKey = "__type_check"
                                let typeCheckName = SQL.SQLName typeCheckKey
                                Seq.singleton (typeCheckName, (Set.singleton typeCheckKey, SQL.CMCheck (String.comparable checkExpr)))

                        let typeIndexKey = sprintf "__type_index__%O" entity.Root.Name
                        let typeIndexName = SQL.SQLName <| sprintf "__type_index__%s" entity.HashName
                        let subEntityIndex =
                            { Columns = [| defaultIndexColumn sqlFunSubEntity |]
                              IncludedColumns = [||]
                              IsUnique = false
                              Predicate = None
                              AccessMethod = defaultIndexType
                            } : SQL.IndexMeta
                        let indexes = Seq.singleton (typeIndexName, (Set.singleton typeIndexKey, subEntityIndex))

                        (columns, constrs, indexes)

                let makeColumn (name, field : ResolvedColumnField) =
                    let fieldRef = { Entity = entityRef; Name = name }
                    let (column, constrs) = makeColumnFieldMeta fieldRef entity field
                    let keys =
                        if entityRef = { Schema = OzmaQLName "public"; Name = OzmaQLName "events" } && name = OzmaQLName "row_id" then
                            Set.singleton "entity_id"
                        else
                            Set.empty
                    (field.ColumnName, keys, column, constrs)

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (name, keys, column, constrs) -> (name, (keys, column)))
                let columnConstraints = columnObjects |> Seq.collect (fun (name, keys, column, constrs) -> constrs)

                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint id)
                let constraints = Seq.concat [idConstraints; subEntityConstraints; columnConstraints; checkConstraintObjects] |> Map.ofSeqUnique

                let columns =  Seq.concat [idColumns; subEntityColumns; userColumns; materializedFieldColumns] |> Map.ofSeq
                let tableObjects =
                    { Table = Some Set.empty
                      TableColumns = columns
                      Constraints = constraints
                      Triggers = Map.empty
                    } : SQL.TableObjectsMeta

                let allIndexes = subEntityIndexes
                let indexObjects = Seq.map (fun (name, (keys, index)) -> (name, (SQL.OMIndex (keys, tableName.Name, index)))) allIndexes
                // Correlation step for raw meta should name primary sequences in the same way.
                let idSeqKey = sprintf "__idseq__%O" entity.Root.Name
                let idObject = Seq.singleton (idSeqName, (SQL.OMSequence (Set.singleton idSeqKey)))
                let extraObjects = Seq.concat [idObject; indexObjects]
                (tableObjects, extraObjects)
            | Some parent ->
                let checkExpr = makeCheckExpr subEntityColumn layout entityRef

                let makeColumn (name, field : ResolvedColumnField) =
                    if Option.isSome field.InheritedFrom then
                        None
                    else
                        let fieldRef = { Entity = entityRef; Name = name }
                        let (meta, constrs) = makeColumnFieldMeta fieldRef entity field
                        let (meta, extraConstrs) =
                            match checkExpr with
                            | None -> (meta, Seq.empty)
                            | _ when meta.IsNullable -> (meta, Seq.empty)
                            | Some check ->
                                // We do not check that values are NULL if row is not of this entity subtype.
                                // This is to optimize insertion and adding of new columns in case of `DefaultExpr` values set.
                                // Say, one adds a new subtype column with `DEFAULT` set -- because we use native PostgreSQL DEFAULT and allow values to be whatever for non-subtypes,
                                // PostgreSQL can instantly initialize the column with default values for all rows, even not of this subtype.
                                let checkNull = SQL.VEIsNull (SQL.VEColumn { Table = None; Name = field.ColumnName })
                                let expr = SQL.VENot (SQL.VEAnd (check, checkNull))
                                let notnullName = SQL.SQLName <| sprintf "__notnull__%s__%s" entity.HashName field.HashName
                                let notnullKey = sprintf "__notnull__%O__%O__%O" entityRef.Schema entityRef.Name name
                                let extraConstrs = Seq.singleton (notnullName, (Set.singleton notnullKey, SQL.CMCheck (String.comparable expr)))
                                ({ meta with IsNullable = true }, extraConstrs)
                        Some (field.ColumnName, Set.empty, meta, Seq.append constrs extraConstrs)

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (name, keys, column, constrs) -> (name, (keys, column)))
                let columnConstraints = columnObjects |> Seq.collect (fun (name, keys, column, constrs) -> constrs)

                let modify =
                    match checkExpr with
                    | Some check -> fun expr -> SQL.VEOr (SQL.VENot check, expr)
                    | None -> id
                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint modify)
                let constraints = Seq.concat [checkConstraintObjects; columnConstraints] |> Map.ofSeqUnique

                let columns = Seq.concat [userColumns; materializedFieldColumns] |> Map.ofSeq
                let tableObjects =
                    { Table = None
                      TableColumns = columns
                      Constraints = constraints
                      Triggers = Map.empty
                    } : SQL.TableObjectsMeta

                (tableObjects, Seq.empty)

        let makeUniqueConstraint (name, constr : ResolvedUniqueConstraint) =
            let key = sprintf "__unique__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = uniqueConstraintSQLName entity.HashName constr.HashName
            (sqlName, (Set.singleton key, makeUniqueConstraintMeta entity constr))
        let uniqueConstraints = entity.UniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint

        let makeIndex (name, index : ResolvedIndex) =
            let key = sprintf "__index__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = indexSQLName entity.HashName index.HashName
            (sqlName, (Set.singleton key, makeIndexMeta entityRef entity index))
        let indexes = entity.Indexes |> Map.toSeq |> Seq.map makeIndex

        let makeReferenceIndex (name, field : ResolvedColumnField) =
            match field with
            | { InheritedFrom = None; FieldType = FTScalar (SFTReference (refEntityRef, opts)) } ->
                // We build indexes for all references to speed up non-local check constraint integrity lookups,
                // and DELETE operations on related fields (PostgreSQL will make use of this index internally, that's why we don't set a predicate).
                let key = sprintf "__refindex__%O__%O__%O" entityRef.Schema entityRef.Name name
                let sqlName = SQL.SQLName <| sprintf "__refindex__%s__%s" entity.HashName field.HashName
                let refIndex =
                    { Columns = [| defaultIndexColumn field.ColumnName |]
                      IncludedColumns = [||]
                      IsUnique = false
                      Predicate = None
                      AccessMethod = defaultIndexType
                    } : SQL.IndexMeta
                Some (sqlName, (Set.singleton key, refIndex))
            | _ -> None
        let refIndexes = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeReferenceIndex

        let allCommonIndexes = Seq.concat [indexes; refIndexes]
        let commonIndexObjects = Seq.map (fun (name, (keys, index)) -> (name, SQL.OMIndex (keys, tableName.Name, index))) allCommonIndexes

        let tableObjects =
            { tableObjects with
                Constraints = Seq.fold (fun constrs (name, value) -> Map.addUnique name value constrs) tableObjects.Constraints uniqueConstraints
            }
        let allExtraObjects = Seq.concat [commonIndexObjects; extraObjects]

        (tableObjects, allExtraObjects)

    let makeSchemaMeta (schemaName : SchemaName) (schema : ResolvedSchema) : Map<ResolvedEntityRef, SQL.TableObjectsMeta> * SQL.SchemaMeta =
        let makeEntity (entityName, entity : ResolvedEntity) =
            let ref = { Schema = schemaName; Name = entityName }
            (entity.Root, makeEntityMeta ref entity)
        let ret = schema.Entities |> Map.toSeq |> Seq.map makeEntity |> Seq.cache
        let tables = ret |> Seq.map (fun (root, (table, objects)) -> (root, table)) |> Map.ofSeqWith (fun name -> SQL.unionTableObjectsMeta)
        let objects = ret |> Seq.collect (fun (root, (table, objects)) -> objects) |> Map.ofSeq
        (tables, { SQL.emptySchemaMeta with Relations = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (compileName ref.Schema, Map.singleton (compileName ref.Name) (SQL.OMTable table))

        let ret = subLayout.Schemas |> Map.toSeq |> Seq.map makeSchema |> Seq.cache
        // Only non-table objects; we need to merge tables.
        let schemas = ret |> Seq.map (fun (name, (tables, objects)) -> (name, (Set.empty, objects))) |> Map.ofSeq
        assert Set.isSubset (Map.keysSet schemas) (subLayout.Schemas |> Map.keys |> Seq.map compileName |> Set.ofSeq)
        // Tables separately.
        let tables = ret |> Seq.map (fun (name, (tables, objects)) -> tables) |> Seq.fold (Map.unionWith SQL.unionTableObjectsMeta) Map.empty
        let splitTables = tables |> Map.toSeq |> Seq.map makeSplitTable |> Map.ofSeqWith (fun name -> Map.unionUnique)
        let mergeTables (schemaName : SQL.SQLName) (keys : SQL.MigrationKeysSet, schema : SQL.SchemaMeta) =
            match Map.tryFind schemaName splitTables with
            | None -> (keys, schema)
            | Some extras ->
                let schema = { schema with Relations = Map.union schema.Relations extras }
                (keys, schema)
        let schemas = Map.map mergeTables schemas

        let schemas = schemas |> Map.filter (fun name (keys, meta) -> not <| SQL.schemaMetaIsEmpty meta)

        { Schemas = schemas
          Extensions = funExtensions
        }

    member this.BuildLayoutMeta = buildLayoutMeta

// Sub-layout allows one to build meta only for a part of
let buildLayoutMeta (layout : Layout) (subLayout : Layout) : SQL.DatabaseMeta =
    let builder = MetaBuilder (layout)
    builder.BuildLayoutMeta subLayout
