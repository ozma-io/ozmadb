module FunWithFlags.FunDB.Layout.Meta

open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
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
          AccessMethod = SQL.SQLName <| index.Type.ToFunQLString()
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
                | FTScalar (SFTReference entityRef) ->
                    // FIXME: support restrictions!
                    let refEntity = layout.FindEntity entityRef |> Option.get
                    let tableRef = compileResolvedEntityRef refEntity.Root
                    let constrKey = sprintf "__foreign__%O__%O__%O" ref.Entity.Schema ref.Entity.Name ref.Name
                    let constrName = SQL.SQLName <| sprintf "__foreign__%s__%s"  entity.HashName field.HashName
                    Seq.singleton (constrName, (Set.singleton constrKey, SQL.CMForeignKey (tableRef, [| (field.ColumnName, sqlFunId) |], SQL.DCDeferrable false)))
                | FTScalar (SFTEnum vals) ->
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
                    let constrKey = sprintf "__enum__%O__%O__%O" ref.Entity.Schema ref.Entity.Name ref.Name
                    let constrName = SQL.SQLName <| sprintf "__enum__%s__%s" entity.HashName field.HashName
                    Seq.singleton (constrName, (Set.singleton constrKey, SQL.CMCheck (String.comparable expr)))
                | _ -> Seq.empty
        (res, constr)

    let makeMaterializedComputedFieldMeta (ref : ResolvedFieldRef) (entity : ResolvedEntity) (field : ResolvedComputedField) : SQL.ColumnMeta =
        let rootInfo = Option.get field.Root
        let columnType =
            if rootInfo.IsLocal then
                let resolved = makeSingleFieldExpr ref.Entity { Entity = None; Name = ref.Name }
                let (arguments, compiled) = compileSingleFieldExpr layout materializedCompilationFlags emptyArguments resolved
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

    let makeCheckConstraintMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (modifyExpr : SQL.ValueExpr -> SQL.ValueExpr) (name : ConstraintName) (constr : ResolvedCheckConstraint) : (SQL.SQLName * (SQL.MigrationKeysSet * SQL.ObjectMeta)) seq =
        let tableName = compileResolvedEntityRef entity.Root
        if not constr.IsLocal then
            Seq.empty
        else
            let expr = modifyExpr <| compileRelatedExpr constr.Expression
            let meta = SQL.CMCheck (String.comparable expr)
            let sqlKey = sprintf "__check__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = SQL.SQLName <| sprintf "__check__%s__%s" entity.HashName constr.HashName
            Seq.singleton (sqlName, (Set.singleton sqlKey, SQL.OMConstraint (tableName.Name, meta)))

    let makeEntityMeta (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : SQL.TableMeta * (SQL.SQLName * (SQL.MigrationKeysSet * SQL.ObjectMeta)) seq =
        let tableName = compileResolvedEntityRef entity.Root

        let makeEntityCheckConstraint modifyExpr (name, constr) = makeCheckConstraintMeta entityRef entity modifyExpr name constr

        let makeMaterializedComputedField (name, maybeField) =
            match maybeField with
            | Ok field ->
                if not field.IsMaterialized || Option.isNone field.Root then
                    None
                else
                    let fieldRef = { Entity = entityRef; Name = name }
                    Some (field.ColumnName, (Set.empty, makeMaterializedComputedFieldMeta fieldRef entity field))
            | Error _ -> None
        let materializedFieldColumns = entity.ComputedFields |> Map.toSeq |> Seq.mapMaybe makeMaterializedComputedField

        let (table, extraObjects) =
            match entity.Parent with
            | None ->
                let idSeqName = SQL.SQLName <| sprintf "__idseq__%s" entity.HashName
                let idConstraints =
                    // Correlation step for raw meta should name primary constraints in the same way.
                    let key = sprintf "__primary__%O" entity.Root.Name
                    let name = SQL.SQLName <| sprintf "__primary__%s" entity.HashName
                    let constr = SQL.CMPrimaryKey ([| sqlFunId |], SQL.DCNotDeferrable)
                    Seq.singleton (name, (Set.singleton key, constr))
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

                        let checkExpr = makeCheckExpr subEntityColumn layout entityRef

                        let typeCheckKey = sprintf "__type_check__%O" entity.Root.Name
                        let typeCheckName = SQL.SQLName <| sprintf "__type_check__%s" entity.HashName
                        let constrs = Seq.singleton (typeCheckName, (Set.singleton typeCheckKey, SQL.CMCheck (String.comparable checkExpr)))
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
                    (field.ColumnName, Set.empty, column, constrs)

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (name, keys, column, constrs) -> (name, (keys, column)))
                let columnConstraints = columnObjects |> Seq.collect (fun (name, keys, column, constrs) -> constrs)

                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint id)

                let table = { Columns = Seq.concat [idColumns; subEntityColumns; userColumns; materializedFieldColumns] |> Map.ofSeq } : SQL.TableMeta

                let constraints = Seq.concat [idConstraints; subEntityConstraints; columnConstraints]
                let constraintObjects = Seq.map (fun (name, (keys, constr)) -> (name, (keys, SQL.OMConstraint (tableName.Name, constr)))) constraints
                let allIndexes = subEntityIndexes
                let indexObjects = Seq.map (fun (name, (keys, index)) -> (name, (keys, SQL.OMIndex (tableName.Name, index)))) allIndexes
                // Correlation step for raw meta should name primary sequences in the same way.
                let idSeqKey = sprintf "__idseq__%O" entity.Root.Name
                let idObject = Seq.singleton (idSeqName, (Set.singleton idSeqKey, SQL.OMSequence))
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
                                let notnullKey = sprintf "__notnull__%O__%O__%O" entityRef.Schema entityRef.Name name
                                Seq.singleton (notnullName, (Set.singleton notnullKey, SQL.CMCheck (String.comparable expr)))
                        Some (field.ColumnName, Set.empty, { meta with IsNullable = true }, Seq.append constrs extraConstrs)

                let columnObjects = entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe makeColumn |> Seq.cache
                let userColumns = columnObjects |> Seq.map (fun (name, keys, column, constrs) -> (name, (keys, column)))
                let columnConstraints = columnObjects |> Seq.collect (fun (name, keys, column, constrs) -> constrs)

                let modify expr = SQL.VEOr (SQL.VENot checkExpr, expr)
                let checkConstraintObjects = entity.CheckConstraints |> Map.toSeq |> Seq.collect (makeEntityCheckConstraint modify)

                let table = { Columns = Seq.concat [userColumns; materializedFieldColumns] |> Map.ofSeq } : SQL.TableMeta

                let constraints = columnConstraints
                let constraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.Name, constr)))) constraints
                let objects = Seq.concat [constraintObjects; checkConstraintObjects]
                (table, objects)

        let makeUniqueConstraint (name, constr : ResolvedUniqueConstraint) =
            let key = sprintf "__unique__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = SQL.SQLName <| sprintf "__unique__%s__%s" entity.HashName constr.HashName
            (sqlName, (Set.singleton key, makeUniqueConstraintMeta entity constr))
        let uniqueConstraints = entity.UniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
        let commonConstraintObjects = Seq.map (fun (key, (name, constr)) -> (key, (name, SQL.OMConstraint (tableName.Name, constr)))) uniqueConstraints

        let makeIndex (name, index : ResolvedIndex) =
            let key = sprintf "__index__%O__%O__%O" entityRef.Schema entityRef.Name name
            let sqlName = SQL.SQLName <| sprintf "__index__%s__%s" entity.HashName index.HashName
            (sqlName, (Set.singleton key, makeIndexMeta entityRef entity index))
        let indexes = entity.Indexes |> Map.toSeq |> Seq.map makeIndex

        let makeReferenceIndex (name, field : ResolvedColumnField) =
            match field with
            | { InheritedFrom = None; FieldType = FTScalar (SFTReference refEntityRef) } ->
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
        (tables, { Objects = objects })

    let buildLayoutMeta (subLayout : Layout) : SQL.DatabaseMeta =
        let makeSchema (name, schema) =
            (compileName name, makeSchemaMeta name schema)
        let makeSplitTable (ref, table) =
            (compileName ref.Schema, Map.singleton (compileName ref.Name) (Set.empty, SQL.OMTable table))

        let ret = subLayout.Schemas |> Map.toSeq |> Seq.map makeSchema |> Seq.cache
        // Only non-table objects; we need to merge tables.
        let schemas = ret |> Seq.map (fun (name, (tables, objects)) -> (name, (Set.empty, objects))) |> Map.ofSeq
        assert Set.isSubset (Map.keysSet schemas) (subLayout.Schemas |> Map.keys |> Seq.map compileName |> Set.ofSeq)
        // Tables separately.
        let tables = ret |> Seq.map (fun (name, (tables, objects)) -> tables) |> Seq.fold (Map.unionWith (fun name -> SQL.unionTableMeta)) Map.empty
        let splitTables = tables |> Map.toSeq |> Seq.map makeSplitTable |> Map.ofSeqWith (fun name -> Map.union)
        let mergeTables (schemaName : SQL.SQLName) (keys : SQL.MigrationKeysSet, schema : SQL.SchemaMeta) =
            match Map.tryFind schemaName splitTables with
            | None -> (keys, schema)
            | Some extras ->
                let schema = { schema with Objects = Map.union schema.Objects extras }
                (keys, schema)
        let schemas = Map.map mergeTables schemas

        let schemas = schemas |> Map.filter (fun name (keys, schema) -> not <| Map.isEmpty schema.Objects)

        { Schemas = schemas
          Extensions = funExtensions
        }

    member this.BuildLayoutMeta = buildLayoutMeta

// Sub-layout allows one to build meta only for a part of
let buildLayoutMeta (layout : Layout) (subLayout : Layout) : SQL.DatabaseMeta =
    let builder = MetaBuilder (layout)
    builder.BuildLayoutMeta subLayout
