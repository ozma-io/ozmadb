module OzmaDB.Operations.SaveRestore

open System.Collections.Generic
open System.Linq
open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open System.IO
open System.IO.Compression
open System.Runtime.Serialization
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open YamlDotNet.Serialization.NamingConventions
open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.OzmaUtils.IO
open OzmaDB.OzmaUtils.Parsing
open OzmaDB.OzmaUtils.Serialization.Yaml
open OzmaDB.OzmaUtils.Serialization.Utils
open OzmaDBSchema.System
open OzmaDB.Exception
open OzmaDB.Connection
open OzmaDB.Operations.Update
open OzmaDB.OzmaQL.Utils
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Compile
open OzmaDB.Layout.Source
open OzmaDB.Layout.Types
open OzmaDB.Layout.Schema
open OzmaDB.Layout.Update
open OzmaDB.Permissions.Source
open OzmaDB.Permissions.Schema
open OzmaDB.Permissions.Update
open OzmaDB.UserViews.Source
open OzmaDB.UserViews.Schema
open OzmaDB.UserViews.Update
open OzmaDB.Attributes.Source
open OzmaDB.Attributes.Schema
open OzmaDB.Attributes.Update
open OzmaDB.Modules.Types
open OzmaDB.Modules.Source
open OzmaDB.Modules.Schema
open OzmaDB.Modules.Update
open OzmaDB.Actions.Types
open OzmaDB.Actions.Source
open OzmaDB.Actions.Schema
open OzmaDB.Actions.Update
open OzmaDB.Triggers.Types
open OzmaDB.Triggers.Source
open OzmaDB.Triggers.Schema
open OzmaDB.Triggers.Update

module SQL = OzmaDB.SQL.AST
module SQL = OzmaDB.SQL.DDL

type RestoreSchemaException(message: string, innerException: exn, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: exn) =
        RestoreSchemaException(message, innerException, isUserException innerException)

    new(message: string) = RestoreSchemaException(message, null, true)

type PrettyColumnField =
    { Type: string
      DefaultValue: string option
      IsNullable: bool
      IsImmutable: bool
      DefaultAttributes: SourceAttributesField option
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

type PrettyComputedField =
    { Expression: string
      AllowBroken: bool
      IsVirtual: bool
      IsMaterialized: bool
      DefaultAttributes: SourceAttributesField option
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

type PrettyEntity =
    { ColumnFields: Map<FieldName, PrettyColumnField>
      ComputedFields: Map<FieldName, PrettyComputedField>
      UniqueConstraints: Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints: Map<ConstraintName, SourceCheckConstraint>
      Indexes: Map<IndexName, SourceIndex>
      MainField: FieldName option
      SaveRestoreKey: ConstraintName option
      IsAbstract: bool
      IsFrozen: bool
      Parent: ResolvedEntityRef option
      SystemDefaultAttributes: Map<FieldName, SourceAttributesField>
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

type PrettyTriggerMeta =
    { AllowBroken: bool
      [<DefaultValue(0)>]
      Priority: int
      Time: TriggerTime
      OnInsert: bool
      OnUpdateFields: FieldName[]
      OnDelete: bool }

type PrettyActionMeta = { AllowBroken: bool }

let private emptyPrettyActionMeta = { AllowBroken = false }: PrettyActionMeta

type PrettyModuleMeta = { AllowBroken: bool }

let private emptyModuleActionMeta = { AllowBroken = false }: PrettyModuleMeta

type PrettyUserViewMeta = { AllowBroken: bool }

let private emptyPrettyUserViewMeta = { AllowBroken = false }: PrettyUserViewMeta

type PrettyUserViewsGeneratorScriptMeta = { AllowBroken: bool }

let private emptyPrettyUserViewsGeneratorScriptMeta =
    { AllowBroken = false }: PrettyUserViewsGeneratorScriptMeta

type CustomEntitiesMap = Map<SchemaName, Map<EntityName, JsonMap[]>>

type SchemaDump =
    { Entities: Map<EntityName, SourceEntity>
      Roles: Map<RoleName, SourceRole>
      UserViews: Map<UserViewName, SourceUserView>
      UserViewsGeneratorScript: SourceUserViewsGeneratorScript option
      DefaultAttributes: Map<SchemaName, SourceAttributesSchema>
      Modules: Map<ModulePath, SourceModule>
      Actions: Map<ActionName, SourceAction>
      Triggers: Map<SchemaName, SourceTriggersSchema>
      CustomEntities: CustomEntitiesMap
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

let schemaDumpHasNoSchema (dump: SchemaDump) =
    Map.isEmpty dump.Entities
    && Map.isEmpty dump.Roles
    && Map.isEmpty dump.UserViews
    && Option.isNone dump.UserViewsGeneratorScript
    && Map.isEmpty dump.DefaultAttributes
    && Map.isEmpty dump.Modules
    && Map.isEmpty dump.Actions
    && Map.isEmpty dump.Triggers

let schemaDumpIsEmpty (dump: SchemaDump) =
    schemaDumpHasNoSchema dump
    && Map.forall (fun schemaName -> Map.forall (fun entityName entries -> Array.isEmpty entries)) dump.CustomEntities

let emptySchemaDump: SchemaDump =
    { Entities = Map.empty
      Roles = Map.empty
      UserViews = Map.empty
      UserViewsGeneratorScript = None
      DefaultAttributes = Map.empty
      Modules = Map.empty
      Actions = Map.empty
      Triggers = Map.empty
      CustomEntities = Map.empty
      Description = ""
      Metadata = JsonMap.empty }

let mergeSchemaDump (a: SchemaDump) (b: SchemaDump) : SchemaDump =
    { Entities = Map.unionUnique a.Entities b.Entities
      Roles = Map.unionUnique a.Roles b.Roles
      UserViews = Map.unionUnique a.UserViews b.UserViews
      UserViewsGeneratorScript = Option.unionUnique a.UserViewsGeneratorScript b.UserViewsGeneratorScript
      DefaultAttributes = Map.unionWith mergeSourceAttributesSchema a.DefaultAttributes b.DefaultAttributes
      Modules = Map.unionUnique a.Modules b.Modules
      Actions = Map.unionUnique a.Actions b.Actions
      Triggers = Map.unionWith mergeSourceTriggersSchema a.Triggers b.Triggers
      CustomEntities = Map.unionWith Map.unionUnique a.CustomEntities b.CustomEntities
      Description = unionDescription a.Description b.Description
      Metadata = unionMetadata a.Metadata b.Metadata }

let private getSchemaByName
    (connection: DatabaseTransaction)
    (schemaName: SchemaName)
    (cancellationToken: CancellationToken)
    =
    task {
        let schemaStr = string schemaName

        let! schemas =
            connection.System.Schemas
                .Where(fun schema -> schema.Name = schemaStr)
                .ToArrayAsync(cancellationToken)

        if Array.isEmpty schemas then
            failwithf "Schema %O not found" schemaName

        return schemas.[0]
    }

type private CustomEntityValueSource =
    | CVSColumn of int
    | CVSRef of ResolvedEntityRef * CustomEntityObjectSource
    | CVSSchemaId

and private CustomEntityObjectSource = Map<FieldName, CustomEntityValueSource>

let private subEntityKey = "sub_entity"

let private schemaArgName = OzmaQLName "schema_id"
let private schemaArg = PLocal schemaArgName

let private schemaArgType =
    requiredArgument (FTScalar(SFTReference(schemasNameFieldRef.Entity, None)))

type private FlatColumn = ResolvedFieldRef list

type private ColumnSourceBuilder(layout: Layout, firstIndex: int) =
    let mutable schemaNamePath = None
    let mutable columns = List()

    let rec getColumnSource
        (isSchemaKey: bool)
        (parentPath: FlatColumn)
        (fieldRef: ResolvedFieldRef)
        (field: ResolvedColumnField)
        : CustomEntityValueSource =
        let path = fieldRef :: parentPath

        match field.FieldType with
        | FTScalar(SFTReference(refEntityRef, opts)) ->
            // First non-reference in (nested) save-restore keys is always schema name.
            if isSchemaKey && refEntityRef = schemasNameFieldRef.Entity then
                assert (Option.isNone schemaNamePath)
                schemaNamePath <- path |> List.rev |> Some
                CVSSchemaId
            else
                let refEntity = layout.FindEntity refEntityRef |> Option.get
                let refKey = Option.get refEntity.SaveRestoreKey
                let refConstr = Map.find refKey refEntity.UniqueConstraints

                let sourceRef =
                    getKeyColumnSources isSchemaKey path refEntityRef refEntity refConstr

                CVSRef(refEntityRef, sourceRef)
        | _ ->
            let idx = firstIndex + columns.Count
            let col = List.rev path
            columns.Add(col)
            CVSColumn idx

    and getKeyColumnSources
        (isSchemaKey: bool)
        (path: FlatColumn)
        (entityRef: ResolvedEntityRef)
        (entity: ResolvedEntity)
        (constr: ResolvedUniqueConstraint)
        : CustomEntityObjectSource =
        let go (i, fieldName) =
            let field = Map.find fieldName entity.ColumnFields
            let fieldRef = { Entity = entityRef; Name = fieldName }
            let source = getColumnSource (i = 0 && isSchemaKey) path fieldRef field
            (fieldName, source)

        constr.Columns |> Seq.indexed |> Seq.map go |> Map.ofSeq

    member this.GetColumnSource isSchemaKey (fieldRef: ResolvedFieldRef) =
        let entity = layout.FindEntity fieldRef.Entity |> Option.get
        let field = Map.find fieldRef.Name entity.ColumnFields
        getColumnSource isSchemaKey [] fieldRef field

    member this.GetKeyColumnSources isSchemaKey entityRef constr =
        let entity = layout.FindEntity entityRef |> Option.get
        getKeyColumnSources isSchemaKey [] entityRef entity constr

    member this.SchemaNamePath = schemaNamePath
    member this.Columns = columns :> IReadOnlyList<FlatColumn>

type private PreparedSaveRestoreKey =
    { Ref: ResolvedEntityRef
      Fields: Set<FieldName>
      FlatColumns: FlatColumn[]
      SchemaNamePath: FlatColumn option
      Sources: CustomEntityObjectSource }

let private prepareSaveRestoreKey (layout: Layout) (entityRef: ResolvedEntityRef) : PreparedSaveRestoreKey =
    let entity = layout.FindEntity entityRef |> Option.get
    let key = Option.get entity.SaveRestoreKey
    let constr = Map.find key entity.UniqueConstraints
    let builder = ColumnSourceBuilder(layout, 0)
    let keyColumnSources = builder.GetKeyColumnSources true entityRef constr

    { Ref = entityRef
      Fields = Set.ofArray constr.Columns
      FlatColumns = builder.Columns.ToArray()
      SchemaNamePath = builder.SchemaNamePath
      Sources = keyColumnSources }

let private saveOneCustomEntity
    (connection: DatabaseTransaction)
    (layout: Layout)
    (entityRef: ResolvedEntityRef)
    (key: PreparedSaveRestoreKey)
    (schemaId: int)
    (cancellationToken: CancellationToken)
    (f: IAsyncEnumerable<JsonMap> -> Task<'a>)
    : Task<'a> =
    task {
        let builder = ColumnSourceBuilder(layout, key.FlatColumns.Length)

        let getData fieldName =
            if Set.contains fieldName key.Fields then
                None
            else
                let fieldRef = { Entity = entityRef; Name = fieldName }
                let source = builder.GetColumnSource false fieldRef
                Some(fieldName, source)

        let entity = layout.FindEntity entityRef |> Option.get

        let dataColumnSources =
            entity.ColumnFields |> Map.keys |> Seq.mapMaybe getData |> Map.ofSeq

        let combinedSources = Map.unionUnique key.Sources dataColumnSources

        let getReference: ResolvedFieldRef list -> ResolvedFieldExpr =
            function
            | [] -> failwith "Impossible"
            | fieldRef :: fullPath ->
                let path =
                    fullPath
                    |> Seq.map (fun fieldRef -> { Name = fieldRef.Name; AsRoot = false })
                    |> Seq.toArray

                let boundPath = fullPath |> Seq.map (fun fieldRef -> fieldRef.Entity) |> Seq.toArray

                let boundMeta =
                    { simpleColumnMeta entityRef with
                        IsInner = false
                        Path = path
                        PathEntities = boundPath }

                let plainRef = relaxFieldRef fieldRef
                makeSingleFieldExpr layout boundMeta plainRef

        let whereExpr =
            match key.SchemaNamePath with
            | None -> None
            | Some schemaPath ->
                let schemaExpr =
                    FERef
                        { Ref =
                            { Ref = VRArgument schemaArg
                              Path = [||]
                              AsRoot = false }
                          Extra = ObjectMap.empty }

                Some <| FEBinaryOp(getReference schemaPath, BOEq, schemaExpr)

        let resultColumns =
            Seq.append key.FlatColumns builder.Columns
            |> Seq.map (getReference >> queryColumnResult >> QRExpr)
            |> Seq.toArray

        let from =
            FEntity
                { fromEntity (relaxEntityRef entityRef) with
                    Only = true }

        let singleSelect =
            { emptySingleSelectExpr with
                Results = resultColumns
                From = Some from
                Where = whereExpr }

        let select = selectExpr (SSelect singleSelect)

        let (maybeCompiledSchemaArg, arguments) =
            match key.SchemaNamePath with
            | None -> (None, emptyArguments)
            | Some _ ->
                let (compiledSchemaArg, arguments) =
                    addArgument schemaArg schemaArgType emptyArguments

                (Some compiledSchemaArg, arguments)

        let (selectInfo, compiled) = compileSelectExpr layout arguments select

        let argumentValues =
            match maybeCompiledSchemaArg with
            | None -> Map.empty
            | Some compiledSchemaArg -> Map.singleton compiledSchemaArg.PlaceholderId (SQL.VInt schemaId)

        let subEntityObject =
            if key.Ref = entityRef then
                None
            else
                Some <| JsonMap.fromObject entityRef

        return!
            connection.Connection.Query.ExecuteQuery (string compiled) argumentValues cancellationToken
            <| fun columns rows ->
                task {
                    let processRow (row: SQL.Value[]) =
                        let rec mapObject (sources: CustomEntityObjectSource) : bool * JsonMap =
                            let obj = JObject()
                            let mutable nonNullFound = false

                            for KeyValue(fieldName, source) in sources do
                                match source with
                                | CVSSchemaId -> ()
                                | CVSColumn i ->
                                    let value = row.[i]

                                    match value with
                                    | SQL.VNull -> ()
                                    | _ -> nonNullFound <- true

                                    obj.[string fieldName] <- JToken.FromObject(value)
                                | CVSRef(ref, refSources) ->
                                    let (innerHasData, inner) = mapObject refSources

                                    if innerHasData then
                                        obj.[string fieldName] <- inner.Map :> JToken
                                        nonNullFound <- true
                                    else
                                        obj.[string fieldName] <- JValue.CreateNull()

                            (nonNullFound, JsonMap obj)

                        let (nonNullFound, obj) = mapObject combinedSources

                        match subEntityObject with
                        | None -> ()
                        | Some subEntity -> obj.Map.[subEntityKey] <- subEntity.Map

                        obj

                    let mapped = rows.Select(processRow)
                    return! f mapped
                }
    }

let private saveCustomEntity
    (connection: DatabaseTransaction)
    (layout: Layout)
    (key: PreparedSaveRestoreKey)
    (schemaId: int)
    (cancellationToken: CancellationToken)
    (f: IAsyncEnumerable<JsonMap> -> Task<'a>)
    : Task<'a seq> =
    task {
        let mutable results = []

        let getOne childRef (entity: ResolvedEntity) =
            task {
                if not entity.IsAbstract then
                    let! ret = saveOneCustomEntity connection layout childRef key schemaId cancellationToken f
                    results <- ret :: results
            }

        let parent = layout.FindEntity key.Ref |> Option.get
        do! getOne key.Ref parent

        for KeyValue(childRef, child) in parent.Children do
            let childEntity = layout.FindEntity childRef |> Option.get
            do! getOne childRef childEntity

        return List.toSeq results
    }

let private saveCustomEntitiesById
    (connection: DatabaseTransaction)
    (layout: Layout)
    (schemaId: int)
    (schemaName: SchemaName)
    (cancellationToken: CancellationToken)
    : Task<CustomEntitiesMap> =
    let go (ref: ResolvedEntityRef) =
        task {
            if ref.Schema = funSchema then
                return None
            else
                let key = prepareSaveRestoreKey layout ref

                match key.SchemaNamePath with
                | None when schemaName <> ref.Schema -> return None
                | _ ->
                    let! rets =
                        saveCustomEntity connection layout key schemaId cancellationToken
                        <| fun rows -> task { return! rows.ToArrayAsync(cancellationToken) }

                    return Some(ref, Array.concat rets)
        }

    layout.SaveRestoredEntities
    |> Seq.mapTask go
    |> Task.map (
        Seq.catMaybes
        >> Seq.groupBy (fun (ref, entries) -> ref.Schema)
        >> Seq.map (fun (schema, entries) ->
            (schema, entries |> Seq.map (fun (ref, entries) -> (ref.Name, entries)) |> Map.ofSeq))
        >> Map.ofSeq
    )

let saveCustomEntities
    (connection: DatabaseTransaction)
    (layout: Layout)
    (schemaName: SchemaName)
    (cancellationToken: CancellationToken)
    : Task<CustomEntitiesMap> =
    task {
        let! schema = getSchemaByName connection schemaName cancellationToken
        return! saveCustomEntitiesById connection layout schema.Id schemaName cancellationToken
    }

let private rawValuesName = OzmaQLName "raw"
let private rawValuesRef = { Schema = None; Name = rawValuesName }: EntityRef

let private indexColumnName = OzmaQLName "__index"
let private rawColumnName i = sprintf "col__%i" i |> OzmaQLName
let private subKeyName i = sprintf "subkey__%i" i |> OzmaQLName

let private isNotNullColumnName (name: FieldName) =
    sprintf "not_null__%O" name |> OzmaQLName

let private checkNotNullData
    (connection: DatabaseTransaction)
    (checkedColumns: (EntityName * EntityName) seq)
    (tableRef: SQL.TableRef)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let buildCheckColumn (fieldName, isNotNullColumn) =
            let isNotNullExpr =
                SQL.VEColumn
                    { Table = None
                      Name = compileName isNotNullColumn }

            let resultExpr =
                SQL.VEColumn
                    { Table = None
                      Name = compileName fieldName }

            SQL.SCExpr(None, SQL.VEOr(SQL.VENot isNotNullExpr, SQL.VEIsNotNull resultExpr))

        let checkColumns = Seq.map buildCheckColumn checkedColumns

        let indexExpr =
            SQL.VEColumn
                { Table = None
                  Name = compileName indexColumnName }

        let indexColumn = SQL.SCExpr(None, indexExpr)

        let singleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = Seq.append (Seq.singleton indexColumn) checkColumns |> Seq.toArray
                From = Some <| SQL.FTable(SQL.fromTable tableRef) }

        let select = SQL.selectExpr (SQL.SSelect singleSelect)

        do!
            connection.Connection.Query.ExecuteQuery (string select) Map.empty cancellationToken
            <| fun columns rows ->
                task {
                    let processRow (row: SQL.Value[]) =
                        for (i, (fieldName, isNotNullColumn)) in Seq.indexed checkedColumns do
                            if not <| SQL.parseBoolValue row.[i + 1] then
                                let index = SQL.parseIntValue row.[0]

                                raisef
                                    RestoreSchemaException
                                    "Error at row %i: failed to find referenced entity for field %s"
                                    index
                                    (toOzmaQLString fieldName)

                    do! rows.ForEachAsync(processRow, cancellationToken)
                }
    }

// Creates temporary table with restored entities.
// One can detect existing entities by `id IS NOT NULL`.
let private loadRestoredRows
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (key: PreparedSaveRestoreKey)
    (schemaId: int)
    (rows: JsonMap seq)
    (cancellationToken: CancellationToken)
    : Task<SQL.TableRef * Set<FieldName>> =
    let addUsedFields usedFields (row: JsonMap) =
        row |> Seq.fold (fun usedFields prop -> Set.add prop.Key usedFields) usedFields

    let usedFields = rows |> Seq.fold addUsedFields Set.empty

    let builder = ColumnSourceBuilder(layout, key.FlatColumns.Length)

    let getData (fieldName, field) =
        if
            Set.contains fieldName key.Fields
            || (fieldIsOptional field && not (Set.contains (string fieldName) usedFields))
        then
            None
        else
            let fieldRef = { Entity = entityRef; Name = fieldName }
            let source = builder.GetColumnSource false fieldRef
            Some(fieldName, source)

    let entity = layout.FindEntity entityRef |> Option.get

    let dataColumnSources =
        entity.ColumnFields |> Map.toSeq |> Seq.mapMaybe getData |> Map.ofSeq

    let combinedSources = Map.unionUnique key.Sources dataColumnSources

    let mutable (arguments, argumentValues) =
        match key.SchemaNamePath with
        | None -> (emptyArguments, Map.empty)
        | Some _ ->
            let (compiledSchemaArg, arguments) =
                addArgument schemaArg schemaArgType emptyArguments

            (arguments, Map.singleton compiledSchemaArg.PlaceholderId (SQL.VInt schemaId))

    let allColumns = Seq.append key.FlatColumns builder.Columns |> Seq.toArray

    let getRawArgument (col: FlatColumn) =
        let finalFieldRef = List.last col
        let finalEntity = layout.FindEntity finalFieldRef.Entity |> Option.get
        let finalField = Map.find finalFieldRef.Name finalEntity.ColumnFields

        { requiredArgument finalField.FieldType with
            Optional = finalField.IsNullable }

    let argumentsRow = allColumns |> Seq.map getRawArgument |> Seq.toArray

    let valueToArgument (arg: ResolvedArgument) (v: FieldValue) : ResolvedFieldExpr =
        let (compiledArg, name, newArguments) = addAnonymousArgument arg arguments
        arguments <- newArguments
        argumentValues <- Map.add compiledArg.PlaceholderId (compileFieldValue v) argumentValues
        resolvedRefFieldExpr <| VRArgument(PLocal name)

    let lookupField (fieldRef: ResolvedFieldRef) =
        let currEntity = layout.FindEntity fieldRef.Entity |> Option.get
        let currField = Map.find fieldRef.Name currEntity.ColumnFields
        (fieldRef, currField)

    let columnsWithFields = allColumns |> Array.map (List.map lookupField)

    let indexArgument = requiredArgument (FTScalar SFTInt)

    let getSourceRow (rowIndex: int, topRow: JsonMap) =
        let rec getRowValue
            (parentPath: FieldName list)
            (attrs: JsonMap)
            : (ResolvedFieldRef * ResolvedColumnField) list -> FieldValue =
            function
            | [] -> failwith "Impossible"
            | [ (fieldRef, field) ] ->
                let inline getPathString () =
                    Seq.rev (fieldRef.Name :: parentPath)
                    |> Seq.map toOzmaQLString
                    |> String.concat "."

                match attrs.TryGetValue(string fieldRef.Name) with
                | (false, _) ->
                    match field.DefaultValue with
                    | Some def -> def
                    | _ when field.IsNullable -> FNull
                    | None ->
                        let currPath = getPathString ()

                        raisef RestoreSchemaException "Error at row %i, path %s: attribute not found" rowIndex currPath
                | (true, attr) ->
                    if attr.Type = JTokenType.Null && not field.IsNullable then
                        let currPath = getPathString ()

                        raisef RestoreSchemaException "Error at row %i, path %s: value cannot be null" rowIndex currPath
                    else
                        match parseValueFromJson field.FieldType attr with
                        | None ->
                            let currPath = getPathString ()

                            raisef
                                RestoreSchemaException
                                "Error at row %i, path %s: cannot convert value to type %O"
                                rowIndex
                                currPath
                                field.FieldType
                        | Some v -> v
            | (fieldRef, field) :: nextPath ->
                let inline getPathString () =
                    Seq.rev (fieldRef.Name :: parentPath)
                    |> Seq.map toOzmaQLString
                    |> String.concat "."

                let inline returnKeyNull () =
                    // Default values are not used for key lookups.
                    if field.IsNullable then
                        FNull
                    else
                        let currPath = getPathString ()

                        raisef RestoreSchemaException "Error at row %i, path %s: attribute not found" rowIndex currPath

                match attrs.TryGetValue(string fieldRef.Name) with
                | (false, _) -> returnKeyNull ()
                | (true, attr) ->
                    match attr with
                    | :? JObject as innerAttrs ->
                        getRowValue (fieldRef.Name :: parentPath) (JsonMap innerAttrs) nextPath
                    | value when value.Type = JTokenType.Null -> returnKeyNull ()
                    | _ ->
                        let currPath = getPathString ()
                        raisef RestoreSchemaException "Error at row %i, path %s: sub-key expected" rowIndex currPath

        let indexColumn = valueToArgument indexArgument (FInt rowIndex)

        let rawValues =
            columnsWithFields
            |> Seq.map (getRowValue [] topRow)
            |> Seq.map2 valueToArgument argumentsRow

        Seq.append (Seq.singleton indexColumn) rawValues
        |> Seq.map VVExpr
        |> Seq.toArray

    let rawValues = rows |> Seq.indexed |> Seq.map getSourceRow |> Seq.toArray
    let rawSelect = SValues rawValues |> selectExpr
    let rawDataColumnNames = seq { 0 .. allColumns.Length - 1 } |> Seq.map rawColumnName

    let rawColumnNames =
        Seq.append (Seq.singleton indexColumnName) rawDataColumnNames |> Seq.toArray

    let rawAlias =
        { Name = rawValuesName
          Fields = Some rawColumnNames }
        : EntityAlias

    let schemaIdExpr = resolvedRefFieldExpr <| VRArgument schemaArg

    let mutable from = FTableExpr <| fromSubSelectExpr rawSelect rawAlias
    let mutable lastSubkeyId = 0
    let mutable checkNotNull = []

    let buildDataColumn (resultName: FieldName) (resultSource: CustomEntityValueSource) =
        let rec getValueColumn (source: CustomEntityValueSource) : ResolvedFieldExpr =
            match source with
            | CVSSchemaId -> schemaIdExpr
            | CVSColumn i ->
                resolvedRefFieldExpr
                <| VRColumn
                    { Entity = Some rawValuesRef
                      Name = rawColumnName i }
            | CVSRef(entityRef, attrs) ->
                let thisKeyName = subKeyName lastSubkeyId
                lastSubkeyId <- lastSubkeyId + 1
                let thisKeyRef = { Schema = None; Name = thisKeyName }: EntityRef

                let referenceFrom =
                    FEntity
                        { fromEntity (relaxEntityRef entityRef) with
                            Alias = Some thisKeyName }

                let buildKeyAttrCheck (subkeyName, subkeySource) =
                    let keyIdExpr = getValueColumn subkeySource

                    let referenceFieldExpr =
                        resolvedRefFieldExpr
                        <| VRColumn
                            { Entity = Some thisKeyRef
                              Name = subkeyName }
                        : ResolvedFieldExpr

                    FEBinaryOp(keyIdExpr, BOEq, referenceFieldExpr)

                let joinCheck =
                    attrs |> Map.toSeq |> Seq.map buildKeyAttrCheck |> Seq.fold1 (curry FEAnd)

                let join =
                    { A = from
                      B = referenceFrom
                      Type = Left
                      Condition = joinCheck }

                from <- FJoin join

                resolvedRefFieldExpr
                <| VRColumn
                    { Entity = Some thisKeyRef
                      Name = funId }

        let dataExpr = getValueColumn resultSource

        let dataColumn =
            { queryColumnResult dataExpr with
                Alias = Some resultName }

        let field = Map.find resultName entity.ColumnFields

        let isNotNullColumn =
            match field.FieldType with
            | FTScalar(SFTReference(refEntityRef, opts)) when field.IsNullable ->
                // We check if any related column is null, because the only way that can happen is if the top-level reference itself is NULL.
                // That's, in turn, because we forbid nullable columns in alternate keys.
                let rec getColumn =
                    function
                    | CVSSchemaId -> None
                    | CVSColumn i -> Some i
                    | CVSRef(entityRef, attrs) -> attrs |> Map.values |> Seq.mapMaybe getColumn |> Seq.first

                let anyColumn = getColumn resultSource |> Option.get |> rawColumnName

                let anyFieldRef =
                    { Entity = Some rawValuesRef
                      Name = anyColumn }
                    : FieldRef

                let anyRef = resolvedRefFieldExpr <| VRColumn anyFieldRef
                let colName = isNotNullColumnName resultName
                checkNotNull <- (resultName, colName) :: checkNotNull

                Seq.singleton
                    { queryColumnResult (FEIsNotNull anyRef) with
                        Alias = Some colName }
            | _ -> Seq.empty

        let cols = Seq.append isNotNullColumn (Seq.singleton dataColumn)
        (dataExpr, cols)

    let sourceRef = relaxEntityRef entityRef

    let sourceIdExpr =
        resolvedRefFieldExpr
        <| VRColumn
            { Entity = Some sourceRef
              Name = funId }
        : ResolvedFieldExpr

    let idColumn = queryColumnResult sourceIdExpr

    let indexExpr =
        resolvedRefFieldExpr
        <| VRColumn
            { Entity = Some rawValuesRef
              Name = indexColumnName }
        : ResolvedFieldExpr

    let indexColumn = queryColumnResult indexExpr

    let dataColumnExprs = combinedSources |> Map.map buildDataColumn
    let dataColumns = dataColumnExprs |> Map.values |> Seq.collect snd

    let resultColumns =
        Seq.append
            (seq {
                indexColumn
                idColumn
            })
            dataColumns
        |> Seq.map QRExpr
        |> Seq.toArray

    let buildKeyAttrCheck (fieldName: FieldName) =
        let (fieldExpr, columns) = Map.find fieldName dataColumnExprs

        let sourceFieldRef =
            resolvedRefFieldExpr
            <| VRColumn
                { Entity = Some sourceRef
                  Name = fieldName }
            : ResolvedFieldExpr

        FEBinaryOp(fieldExpr, BOEq, sourceFieldRef)

    let joinCheck = key.Fields |> Seq.map buildKeyAttrCheck |> Seq.fold1 (curry FEAnd)

    let sourceFrom =
        FEntity
            { fromEntity sourceRef with
                Only = true }

    let sourceJoin =
        { A = from
          B = sourceFrom
          Type = Left
          Condition = joinCheck }

    let singleSelect =
        { emptySingleSelectExpr with
            Results = resultColumns
            From = Some <| FJoin sourceJoin }

    let select = selectExpr (SSelect singleSelect)
    let (selectInfo, compiled) = compileSelectExpr layout arguments select

    let dataTableName = connection.NewAnonymousSQLName "restored_data"
    let dataTableRef = { Schema = None; Name = dataTableName }: SQL.TableRef

    let dataTable =
        { Table = dataTableRef
          Query = compiled
          Columns = None
          Temporary = Some { OnCommit = Some SQL.CADrop } }
        : SQL.CreateTableAsOperation

    task {
        let! _ = connection.Connection.Query.ExecuteNonQuery (string dataTable) argumentValues cancellationToken

        // Check that for all non-NULL values references have been found.
        if not <| List.isEmpty checkNotNull then
            do! checkNotNullData connection checkNotNull dataTableRef cancellationToken

        return (dataTableRef, Map.keysSet dataColumnSources)
    }

let private insertRestoredRows
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (key: PreparedSaveRestoreKey)
    (availableColumns: Set<FieldName>)
    (dataTableRef: SQL.TableRef)
    (idsTableRef: SQL.TableRef)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let allFields = Seq.append key.Fields availableColumns |> Seq.toArray

        let getSourceColumn (fieldName: FieldName) =
            let resultExpr =
                SQL.VEColumn
                    { Table = None
                      Name = compileName fieldName }

            SQL.SCExpr(None, resultExpr)

        let dataColumns = Seq.map getSourceColumn allFields

        let entity = layout.FindEntity entityRef |> Option.get

        let (subEntityFields, subEntityColumns) =
            if not <| hasSubType entity then
                (Seq.empty, Seq.empty)
            else
                let col = SQL.SCExpr(None, SQL.VEValue(SQL.VString entity.TypeName))
                (Seq.singleton sqlFunSubEntity, Seq.singleton col)

        let columns = Seq.append subEntityColumns dataColumns |> Seq.toArray
        let idExpr = SQL.VEColumn { Table = None; Name = sqlFunId }

        let singleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = columns
                From = Some <| SQL.FTable(SQL.fromTable dataTableRef)
                Where = Some <| SQL.VEIsNull idExpr }

        let select = SQL.selectExpr (SQL.SSelect singleSelect)

        let getDataField (fieldName: FieldName) =
            let field = Map.tryFind fieldName entity.ColumnFields |> Option.get
            field.ColumnName

        let dataFields = allFields |> Seq.map getDataField

        let fields =
            Seq.append subEntityFields dataFields
            |> Seq.map (fun col -> (null, col))
            |> Seq.toArray

        let insertedRef = compileResolvedEntityRef entity.Root: SQL.TableRef
        let idExpr = SQL.VEColumn { Table = None; Name = sqlFunId }

        let innerInsert =
            { SQL.insertExpr (SQL.operationTable insertedRef) (SQL.ISSelect select) with
                Columns = fields
                Returning = [| SQL.SCExpr(None, idExpr) |] }

        // Place inserted ids into given temporary table.
        let auxName = SQL.SQLName "aux"
        let auxRef = { Schema = None; Name = auxName }: SQL.TableRef

        let idsSingleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = [| SQL.SCAll None |]
                From = Some <| SQL.FTable(SQL.fromTable auxRef) }

        let idsSelect = SQL.selectExpr (SQL.SSelect idsSingleSelect)

        let idsCTE =
            { SQL.commonTableExpr (SQL.DEInsert innerInsert) with
                Materialized = Some true }

        let outerSelect =
            { SQL.insertExpr (SQL.operationTable idsTableRef) (SQL.ISSelect idsSelect) with
                CTEs = Some <| SQL.commonTableExprs [| (auxName, idsCTE) |]
                Columns = [| (null, sqlFunId) |] }

        let! _ = connection.Connection.Query.ExecuteNonQuery (string outerSelect) Map.empty cancellationToken
        ()
    }

let private updateRestoredRows
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (availableColumns: Set<FieldName>)
    (dataTableRef: SQL.TableRef)
    (cancellationToken: CancellationToken)
    : Task<SQL.TableRef> =
    task {
        let entity = layout.FindEntity entityRef |> Option.get

        let getAssignment (fieldName: FieldName) =
            let field = Map.tryFind fieldName entity.ColumnFields |> Option.get

            let resultExpr =
                SQL.VEColumn
                    { Table = Some dataTableRef
                      Name = compileName fieldName }

            SQL.UAESet(SQL.updateColumnName field.ColumnName, SQL.IVExpr resultExpr)

        let assignments = Seq.map getAssignment availableColumns |> Seq.toArray

        let updatedRef = compileResolvedEntityRef entity.Root: SQL.TableRef

        let updatedIdExpr =
            SQL.VEColumn
                { Table = Some updatedRef
                  Name = sqlFunId }

        let sourceIdExpr =
            SQL.VEColumn
                { Table = Some dataTableRef
                  Name = sqlFunId }

        let idCheck = SQL.VEBinaryOp(sourceIdExpr, SQL.BOEq, updatedIdExpr)

        let update =
            { SQL.updateExpr (SQL.operationTable updatedRef) with
                Assignments = assignments
                From = Some <| SQL.FTable(SQL.fromTable dataTableRef)
                Returning = [| SQL.SCExpr(None, updatedIdExpr) |]
                Where = Some idCheck }

        // Place updated ids into a separate temporary table.
        let auxName = SQL.SQLName "aux"
        let auxRef = { Schema = None; Name = auxName }: SQL.TableRef

        let idsSingleSelect =
            { SQL.emptySingleSelectExpr with
                Columns = [| SQL.SCAll None |]
                From = Some <| SQL.FTable(SQL.fromTable auxRef) }

        let idsCTE =
            { SQL.commonTableExpr (SQL.DEUpdate update) with
                Materialized = Some true }

        let idsSelect =
            { SQL.selectExpr (SQL.SSelect idsSingleSelect) with
                CTEs = Some <| SQL.commonTableExprs [| (auxName, idsCTE) |] }

        let idsTableName = connection.NewAnonymousSQLName "restored_ids"
        let idsTableRef = { Schema = None; Name = idsTableName }: SQL.TableRef

        let idsTable =
            { Table = idsTableRef
              Query = idsSelect
              Columns = None
              Temporary = Some { OnCommit = Some SQL.CADrop } }
            : SQL.CreateTableAsOperation

        let! _ = connection.Connection.Query.ExecuteNonQuery (string idsTable) Map.empty cancellationToken

        return idsTableRef
    }

let private deleteNonRestoredRows
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (key: PreparedSaveRestoreKey)
    (schemaId: int)
    (maybeIdsTableRef: SQL.TableRef option)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let (arguments, argumentValues, schemaCheck) =
            match key.SchemaNamePath with
            | None -> (emptyArguments, Map.empty, None)
            | Some schemaNamePath ->
                let (compiledSchemaArg, arguments) =
                    addArgument schemaArg schemaArgType emptyArguments

                let argumentValues =
                    Map.singleton compiledSchemaArg.PlaceholderId (SQL.VInt schemaId)

                let (schemaKey, schemaPath) =
                    match schemaNamePath with
                    | [] -> failwith "Impossible"
                    | key :: path -> (key, path)

                let boundPath = schemaPath |> Seq.map (fun ref -> ref.Entity) |> Seq.toArray

                let arrowPath =
                    schemaPath
                    |> Seq.map (fun ref -> { Name = ref.Name; AsRoot = false })
                    |> Seq.toArray

                let boundMeta =
                    { simpleColumnMeta schemaKey.Entity with
                        IsInner = false
                        Path = arrowPath
                        PathEntities = boundPath }

                let plainRef = relaxFieldRef schemaKey
                let schemaRefExpr = makeSingleFieldExpr layout boundMeta plainRef
                let schemaArgExpr = resolvedRefFieldExpr <| VRArgument schemaArg
                let schemaCheck = FEBinaryOp(schemaRefExpr, BOEq, schemaArgExpr)

                (arguments, argumentValues, Some schemaCheck)

        let deletedRef = relaxEntityRef entityRef

        let deletedCheck =
            match maybeIdsTableRef with
            | None -> None
            | Some idsTableRef ->
                let idsEntityRef = decompileTableRef idsTableRef

                let sourceIdExpr =
                    resolvedRefFieldExpr
                    <| VRColumn
                        { Entity = Some idsEntityRef
                          Name = funId }
                    : ResolvedFieldExpr

                let singleSelect =
                    { emptySingleSelectExpr with
                        Results = [| QRExpr <| queryColumnResult sourceIdExpr |]
                        From = Some <| FEntity(fromEntity idsEntityRef) }

                let select = selectExpr (SSelect singleSelect)

                let deletedIdExpr =
                    resolvedRefFieldExpr
                    <| VRColumn
                        { Entity = Some deletedRef
                          Name = funId }
                    : ResolvedFieldExpr

                Some <| FENotInQuery(deletedIdExpr, select)

        let delete =
            { deleteExpr
                  { fromEntity deletedRef with
                      Only = true } with
                Where = Option.unionWith (curry FEAnd) schemaCheck deletedCheck }

        let (selectInfo, compiled) = compileDataExpr layout arguments (DEDelete delete)

        let! _ = connection.Connection.Query.ExecuteNonQuery (string compiled) argumentValues cancellationToken
        ()
    }

// We restore entities in two stages:
// 1. In forward order, Load restored data and match with existing records. Then insert new rows and update existing ones;
// 2. In reverse order, remove all rows which haven't been matched.
let private restoreOneCustomEntity
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (key: PreparedSaveRestoreKey)
    (schemaId: int)
    (rows: JsonMap seq)
    (cancellationToken: CancellationToken)
    : Task<unit -> Task> =
    task {
        if Seq.isEmpty rows then
            return fun () -> deleteNonRestoredRows layout connection entityRef key schemaId None cancellationToken
        else
            let! (dataTableRef, availableColumns) =
                loadRestoredRows layout connection entityRef key schemaId rows cancellationToken

            let! idsTableRef =
                updateRestoredRows layout connection entityRef availableColumns dataTableRef cancellationToken

            do!
                insertRestoredRows
                    layout
                    connection
                    entityRef
                    key
                    availableColumns
                    dataTableRef
                    idsTableRef
                    cancellationToken

            return
                fun () ->
                    deleteNonRestoredRows layout connection entityRef key schemaId (Some idsTableRef) cancellationToken
    }

let private restoreCustomEntity
    (layout: Layout)
    (connection: DatabaseTransaction)
    (entityRef: ResolvedEntityRef)
    (schemaId: int)
    (schemaName: SchemaName)
    (maybeRows: JsonMap[] option)
    (cancellationToken: CancellationToken)
    : Task<(unit -> Task) seq> =
    let key = prepareSaveRestoreKey layout entityRef

    if Option.isNone key.SchemaNamePath && Option.isNone maybeRows then
        Task.result Seq.empty
    else
        if Option.isNone key.SchemaNamePath && schemaName <> entityRef.Schema then
            raisef
                RestoreSchemaException
                "Custom entities that don't reference schemas can only exist in the same schema"

        let rows = Option.defaultValue [||] maybeRows

        let getSubEntity (row: JsonMap) =
            match row.TryGetValue(subEntityKey) with
            | (false, _) -> entityRef
            | (true, subEntityToken) ->
                try
                    subEntityToken.ToObject<ResolvedEntityRef>()
                with :? JsonException as e ->
                    raisefUserWithInner RestoreSchemaException e "Failed to parse subentity"

        let mutable leftoverRows = rows |> Seq.map (fun row -> (getSubEntity row, row))

        let getOne (childRef, entity: ResolvedEntity) =
            task {
                if entity.IsAbstract then
                    return Seq.empty
                else
                    let (currRows, newLeftoverRows) =
                        Seq.partition (fun (rowRef, row) -> rowRef = childRef) leftoverRows

                    leftoverRows <- newLeftoverRows
                    let restoredRows = Seq.map snd currRows

                    let! deleteTask =
                        restoreOneCustomEntity layout connection entityRef key schemaId restoredRows cancellationToken

                    return Seq.singleton deleteTask
            }

        let parent = layout.FindEntity entityRef |> Option.get

        let restoredEntities =
            seq {
                yield (entityRef, parent)

                for KeyValue(childRef, child) in parent.Children do
                    let childEntity = layout.FindEntity childRef |> Option.get
                    yield (childRef, childEntity)
            }

        restoredEntities |> Seq.collectTask getOne

type SchemaCustomEntities = Map<SchemaName, CustomEntitiesMap>

let restoreCustomEntities
    (layout: Layout)
    (connection: DatabaseTransaction)
    (allEntitiesMap: SchemaCustomEntities)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let restoredSchemas = allEntitiesMap |> Map.keys |> Seq.map string |> Seq.toArray

        let! schemaIds =
            (query {
                for schema in connection.System.Schemas do
                    where (restoredSchemas.Contains(schema.Name))
            })
                .ToDictionaryAsync((fun schema -> OzmaQLName schema.Name), (fun schema -> schema.Id))

        for KeyValue(srcSchemaName, schemasMap) in allEntitiesMap do
            for KeyValue(schemaName, entitiesMap) in schemasMap do
                for KeyValue(entityName, rows) in entitiesMap do
                    let entityRef =
                        { Schema = schemaName
                          Name = entityName }

                    match layout.FindEntity entityRef with
                    | None ->
                        raisef RestoreSchemaException "In schema %O: entity %O does not exist" schemaName entityRef
                    | Some entity when Option.isNone entity.SaveRestoreKey ->
                        raisef
                            RestoreSchemaException
                            "In schema %O: entity %O is not save-restorable"
                            schemaName
                            entityRef
                    | _ -> ()

        let insertAndUpdateSchema (entityRef: ResolvedEntityRef) (srcSchemaName, rowsMap: CustomEntitiesMap) =
            let schemaId = schemaIds.[srcSchemaName]

            let maybeRows =
                Map.tryFind entityRef.Schema rowsMap |> Option.bind (Map.tryFind entityRef.Name)

            try
                restoreCustomEntity layout connection entityRef schemaId srcSchemaName maybeRows cancellationToken
            with :? RestoreSchemaException as e ->
                raisefUserWithInner RestoreSchemaException e "In schema %O" srcSchemaName

        let insertAndUpdate entityRef =
            allEntitiesMap |> Map.toSeq |> Seq.collectTask (insertAndUpdateSchema entityRef)

        let! deleteActions =
            layout.SaveRestoredEntities
            |> Seq.filter (fun entityRef -> entityRef.Schema <> funSchema)
            |> Seq.collectTask insertAndUpdate

        do! deleteActions |> Seq.rev |> Seq.iterTask (fun action -> action ())
    }

let saveSchema
    (conn: DatabaseTransaction)
    (layout: Layout)
    (schemaName: SchemaName)
    (cancellationToken: CancellationToken)
    : Task<SchemaDump> =
    task {
        let! schema = getSchemaByName conn schemaName cancellationToken

        let schemaCheck =
            Expr.toExpressionFunc <@ fun (someSchema: Schema) -> someSchema.Id = schema.Id @>

        let! entitiesData = buildSchemaLayout conn.System (Some schemaCheck) cancellationToken
        let! rolesData = buildSchemaPermissions conn.System (Some schemaCheck) cancellationToken
        let! userViewsData = buildSchemaUserViews conn.System (Some schemaCheck) cancellationToken
        let! attributesData = buildSchemaAttributes conn.System (Some schemaCheck) cancellationToken
        let! modulesMeta = buildSchemaModules conn.System (Some schemaCheck) cancellationToken
        let! actionsMeta = buildSchemaActions conn.System (Some schemaCheck) cancellationToken
        let! triggersData = buildSchemaTriggers conn.System (Some schemaCheck) cancellationToken
        let! customEntitiesData = saveCustomEntitiesById conn layout schema.Id schemaName cancellationToken

        let findOrFail m =
            match Map.tryFind schemaName m with
            | None -> failwithf "Schema %O not found" schemaName
            | Some v -> v

        let entities = findOrFail entitiesData.Schemas
        let roles = findOrFail rolesData.Schemas
        let userViews = findOrFail userViewsData.Schemas
        let attributes = findOrFail attributesData.Schemas
        let modules = findOrFail modulesMeta.Schemas
        let actions = findOrFail actionsMeta.Schemas
        let triggers = findOrFail triggersData.Schemas

        return
            { Entities = entities.Entities
              Roles = roles.Roles
              UserViews =
                if Option.isSome userViews.GeneratorScript then
                    Map.empty
                else
                    userViews.UserViews
              UserViewsGeneratorScript = userViews.GeneratorScript
              DefaultAttributes = attributes.Schemas
              Modules = modules.Modules
              Actions = actions.Actions
              Triggers = triggers.Schemas
              CustomEntities = customEntitiesData
              Description = schema.Description
              Metadata = JsonMap.parse schema.Metadata }
    }

let restoreSchemas
    (conn: DatabaseTransaction)
    (oldLayout: Layout)
    (dumps: Map<SchemaName, SchemaDump>)
    (cancellationToken: CancellationToken)
    : Task<bool> =
    task {
        let makeSchema name (dump: SchemaDump) : SourceSchema =
            { Entities = dump.Entities
              Description = dump.Description
              Metadata = dump.Metadata }

        let newLayout = { Schemas = Map.map makeSchema dumps }: SourceLayout

        let newPerms =
            { Schemas = dumps |> Map.map (fun name dump -> { Roles = dump.Roles }) }: SourcePermissions

        let makeUserView name (dump: SchemaDump) =
            { UserViews =
                if Option.isSome dump.UserViewsGeneratorScript then
                    Map.empty
                else
                    dump.UserViews
              GeneratorScript = dump.UserViewsGeneratorScript }

        let newUserViews = { Schemas = Map.map makeUserView dumps }: SourceUserViews

        let newAttributes =
            { Schemas = dumps |> Map.map (fun name dump -> { Schemas = dump.DefaultAttributes }) }
            : SourceDefaultAttributes

        let newModules =
            { Schemas = dumps |> Map.map (fun name dump -> { Modules = dump.Modules }) }: SourceModules

        let newActions =
            { Schemas = dumps |> Map.map (fun name dump -> { Actions = dump.Actions }) }: SourceActions

        let newTriggers =
            { Schemas = dumps |> Map.map (fun name dump -> { Schemas = dump.Triggers }) }: SourceTriggers

        let! layoutUpdate = updateLayout conn.System newLayout cancellationToken

        let! permissionsUpdate =
            try
                updatePermissions conn.System newPerms cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore permissions"

        let! userViewsUpdate =
            try
                updateUserViews conn.System newUserViews cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore user views"

        let! attributesUpdate =
            try
                updateAttributes conn.System newAttributes cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore attributes"

        let! modulesUpdate =
            try
                updateModules conn.System newModules cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore modules"

        let! actionsUpdate =
            try
                updateActions conn.System newActions cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore actions"

        let! triggersUpdate =
            try
                updateTriggers conn.System newTriggers cancellationToken
            with :? SystemUpdaterException as e ->
                raisefUserWithInner RestoreSchemaException e "Failed to restore triggers"

        let fullUpdate =
            seq {
                layoutUpdate
                permissionsUpdate
                userViewsUpdate
                attributesUpdate
                actionsUpdate
                triggersUpdate
                modulesUpdate
            }
            |> Seq.fold1 unionUpdateResult

        do! deleteDeferredFromUpdate oldLayout conn fullUpdate cancellationToken
        // Clear EF Core objects from cache, so they will be fetched anew.
        conn.System.ChangeTracker.Clear()

        return not <| updateResultIsEmpty fullUpdate
    }

let private prettifyTriggerMeta (trigger: SourceTrigger) : PrettyTriggerMeta =
    { AllowBroken = trigger.AllowBroken
      Priority = trigger.Priority
      Time = trigger.Time
      OnInsert = trigger.OnInsert
      OnUpdateFields = trigger.OnUpdateFields
      OnDelete = trigger.OnDelete }

let private deprettifyTrigger (meta: PrettyTriggerMeta) (procedure: string) : SourceTrigger =
    { AllowBroken = meta.AllowBroken
      Priority = meta.Priority
      Time = meta.Time
      OnInsert = meta.OnInsert
      OnUpdateFields = meta.OnUpdateFields
      OnDelete = meta.OnDelete
      Procedure = procedure }

let private prettifyColumnField
    (defaultAttrs: SourceAttributesField option)
    (field: SourceColumnField)
    : PrettyColumnField =
    { Type = field.Type
      DefaultValue = field.DefaultValue
      IsNullable = field.IsNullable
      IsImmutable = field.IsImmutable
      DefaultAttributes = defaultAttrs
      Description = field.Description
      Metadata = field.Metadata }

let private deprettifyColumnField (field: PrettyColumnField) : SourceAttributesField option * SourceColumnField =
    let ret =
        { Type = field.Type
          DefaultValue = field.DefaultValue
          IsNullable = field.IsNullable
          IsImmutable = field.IsImmutable
          Description = field.Description
          Metadata = field.Metadata }

    (field.DefaultAttributes, ret)

let private prettifyComputedField
    (defaultAttrs: SourceAttributesField option)
    (field: SourceComputedField)
    : PrettyComputedField =
    { Expression = field.Expression
      AllowBroken = field.AllowBroken
      IsVirtual = field.IsVirtual
      IsMaterialized = field.IsMaterialized
      DefaultAttributes = defaultAttrs
      Description = field.Description
      Metadata = field.Metadata }

let private deprettifyComputedField (field: PrettyComputedField) : SourceAttributesField option * SourceComputedField =
    let ret =
        { Expression = field.Expression
          AllowBroken = field.AllowBroken
          IsVirtual = field.IsVirtual
          IsMaterialized = field.IsMaterialized
          Description = field.Description
          Metadata = field.Metadata }
        : SourceComputedField

    (field.DefaultAttributes, ret)

let private prettifyEntity (defaultAttrs: SourceAttributesEntity) (entity: SourceEntity) : PrettyEntity =
    let applyAttrs fn name = fn (defaultAttrs.FindField name)

    { ColumnFields = Map.map (applyAttrs prettifyColumnField) entity.ColumnFields
      ComputedFields = Map.map (applyAttrs prettifyComputedField) entity.ComputedFields
      UniqueConstraints = entity.UniqueConstraints
      CheckConstraints = entity.CheckConstraints
      Indexes = entity.Indexes
      MainField = entity.MainField
      SaveRestoreKey = entity.SaveRestoreKey
      IsAbstract = entity.IsAbstract
      IsFrozen = entity.IsFrozen
      Parent = entity.Parent
      SystemDefaultAttributes =
        defaultAttrs.Fields
        |> Map.filter (fun name attrs -> Set.contains name systemColumns)
      Description = entity.Description
      Metadata = entity.Metadata }

let private deprettifyEntity (entity: PrettyEntity) : SourceAttributesEntity option * SourceEntity =
    if
        not
        <| Set.isEmpty (Set.difference (Map.keysSet entity.SystemDefaultAttributes) systemColumns)
    then
        raisef RestoreSchemaException "`systemDefaultAttributes` can contain only attributes for system columns"

    let mutable defaultAttrs = entity.SystemDefaultAttributes

    let extractAttrs name (maybeFieldAttrs, entry) =
        match maybeFieldAttrs with
        | Some fieldAttrs -> defaultAttrs <- Map.add name fieldAttrs defaultAttrs
        | None -> ()

        entry

    let ret =
        { ColumnFields = Map.map (fun name -> deprettifyColumnField >> extractAttrs name) entity.ColumnFields
          ComputedFields = Map.map (fun name -> deprettifyComputedField >> extractAttrs name) entity.ComputedFields
          UniqueConstraints = entity.UniqueConstraints
          CheckConstraints = entity.CheckConstraints
          Indexes = entity.Indexes
          MainField = entity.MainField
          InsertedInternally = false
          UpdatedInternally = false
          DeletedInternally = false
          SaveRestoreKey = entity.SaveRestoreKey
          IsHidden = false
          TriggersMigration = false
          IsAbstract = entity.IsAbstract
          IsFrozen = entity.IsFrozen
          Parent = entity.Parent
          Description = entity.Description
          Metadata = entity.Metadata }

    let attrsRet =
        if Map.isEmpty defaultAttrs then
            None
        else
            Some { Fields = defaultAttrs }

    (attrsRet, ret)

let private settingsEntry = "schema.yaml"

let private extraDefaultAttributesEntry = "extra_default_attributes.yaml"

let private userViewsGeneratorMetaEntry = "user_views_generator.yaml"
let private userViewsGeneratorEntry = "user_views_generator.mjs"

let private maxSaveRestoreSize = 32L * 1024L * 1024L // 32MB

let myYamlSerializer =
    makeYamlSerializer
        { defaultYamlSerializerSettings with
            NamingConvention = CamelCaseNamingConvention.Instance }

let myYamlDeserializer =
    makeYamlDeserializer
        { defaultYamlDeserializerSettings with
            NamingConvention = CamelCaseNamingConvention.Instance }

[<SerializeAsObject("type"); NoEquality; NoComparison>]
type SavedSchemaData =
    | [<CaseKey("customEntities")>] SSCustomEntities of CustomEntities: CustomEntitiesMap
    | [<CaseKey("full", Type = CaseSerialization.InnerObject); DefaultCase>] SSFull of Schema: SchemaDump

type PrettySchemaSettings =
    { [<DataMember(EmitDefaultValue = false)>]
      OnlyCustomEntities: bool
      [<DefaultValue("")>]
      Description: string
      Metadata: JsonMap }

let emptyPrettySchemaSettings: PrettySchemaSettings =
    { OnlyCustomEntities = false
      Description = ""
      Metadata = JsonMap.empty }

// This should be called only with a `MemoryStream` or a compatible stream,
// as `ZipArchive` is synchronous!
let schemasToZipFile (schemas: Map<SchemaName, SavedSchemaData>) (stream: Stream) =
    use zip = new ZipArchive(stream, ZipArchiveMode.Create, true)
    let mutable totalSize = 0L

    for KeyValue(schemaName, data) in schemas do
        let useEntry (path: string) (fn: StreamWriter -> unit) =
            let entry = zip.CreateEntry(sprintf "%O/%s" schemaName path)
            // https://superuser.com/questions/603068/unzipping-file-whilst-getting-correct-permissions
            entry.ExternalAttributes <- 0o644 <<< 16
            use writer = new StreamWriter(entry.Open())
            fn writer
            totalSize <- totalSize + writer.BaseStream.Position

        let dumpToEntry (path: string) (document: 'a) =
            useEntry path
            <| fun writer -> myYamlSerializer.Serialize(writer, document :> obj)

        let (dump, onlyCustomEntities) =
            match data with
            | SSCustomEntities entities ->
                ({ emptySchemaDump with
                    CustomEntities = entities },
                 true)
            | SSFull dump -> (dump, false)

        let settings =
            { OnlyCustomEntities = onlyCustomEntities
              Description = dump.Description
              Metadata = dump.Metadata }
            : PrettySchemaSettings

        dumpToEntry "schema.yaml" settings

        for KeyValue(name, entity) in dump.Entities do
            let defaultAttrs =
                dump.DefaultAttributes
                |> Map.tryFind schemaName
                |> Option.bind (fun schema -> Map.tryFind name schema.Entities)
                |> Option.defaultValue emptySourceAttributesEntity

            let prettyEntity = prettifyEntity defaultAttrs entity
            dumpToEntry (sprintf "entities/%O.yaml" name) prettyEntity

        for KeyValue(name, role) in dump.Roles do
            dumpToEntry (sprintf "roles/%O.yaml" name) role

        for KeyValue(name, uv) in dump.UserViews do
            useEntry (sprintf "user_views/%O.funql" name)
            <| fun writer -> writer.Write(uv.Query)

            if uv.AllowBroken then
                let uvMeta = { AllowBroken = true }: PrettyUserViewMeta
                dumpToEntry (sprintf "user_views/%O.yaml" name) uvMeta

        for KeyValue(modulePath, modul) in dump.Modules do
            if not <| modulePath.EndsWith(".mjs") then
                failwithf "Unexpected module path in %O: %s" schemaName modulePath

            useEntry (sprintf "modules/%s" modulePath)
            <| fun writer -> writer.Write(modul.Source)

            if modul.AllowBroken then
                // Module name should always end in .mjs, and we exploit it here.
                let moduleMeta = { AllowBroken = true }: PrettyModuleMeta
                let barePath = String.removeSuffix ".mjs" modulePath
                dumpToEntry (sprintf "modules/%O.yaml" barePath) moduleMeta

        for KeyValue(actionName, action) in dump.Actions do
            useEntry (sprintf "actions/%O.mjs" actionName)
            <| fun writer -> writer.Write(action.Function)

            if action.AllowBroken then
                let actionMeta = { AllowBroken = true }: PrettyActionMeta
                dumpToEntry (sprintf "actions/%O.yaml" actionName) actionMeta

        for KeyValue(schemaName, schemaTriggers) in dump.Triggers do
            for KeyValue(entityName, entityTriggers) in schemaTriggers.Entities do
                for KeyValue(triggerName, trigger) in entityTriggers.Triggers do
                    let prettyMeta = prettifyTriggerMeta trigger
                    dumpToEntry (sprintf "triggers/%O/%O/%O.yaml" schemaName entityName triggerName) prettyMeta

                    useEntry (sprintf "triggers/%O/%O/%O.mjs" schemaName entityName triggerName)
                    <| fun writer -> writer.Write(trigger.Procedure)

        let extraAttributes =
            dump.DefaultAttributes |> Map.filter (fun name schema -> name <> schemaName)

        if not <| Map.isEmpty extraAttributes then
            dumpToEntry extraDefaultAttributesEntry extraAttributes

        match dump.UserViewsGeneratorScript with
        | None -> ()
        | Some script ->
            useEntry userViewsGeneratorEntry <| fun writer -> writer.Write(script)

            if script.AllowBroken then
                let genMeta = { AllowBroken = true }: PrettyUserViewsGeneratorScriptMeta
                dumpToEntry userViewsGeneratorMetaEntry genMeta

        for KeyValue(schemaName, schemaEntries) in dump.CustomEntities do
            for KeyValue(entityName, entityEntries) in schemaEntries do
                if not <| Array.isEmpty entityEntries then
                    dumpToEntry (sprintf "custom/%O/%O.yaml" schemaName entityName) entityEntries

    if totalSize > maxSaveRestoreSize then
        failwithf "Total files size in archive is %i, which is too large" totalSize

// This should be called only with a `MemoryStream` or a compatible stream,
// as `ZipArchive` is synchronous!
let schemasFromZipFile (stream: Stream) : Map<SchemaName, SavedSchemaData> =

    use zip = new ZipArchive(stream, ZipArchiveMode.Read)
    let mutable leftSize = maxSaveRestoreSize

    let readEntry (entry: ZipArchiveEntry) (fn: StreamReader -> 'a) : 'a =
        if leftSize - entry.Length < 0L then
            raisef
                RestoreSchemaException
                "Archive entry %s length is %i, which exceeds allowed max length of %i bytes"
                entry.FullName
                entry.Length
                leftSize

        let stream = new MaxLengthStream(entry.Open(), leftSize)
        use reader = new StreamReader(stream)

        let ret =
            try
                fn reader
            with :? IOException as e ->
                raisefUserWithInner RestoreSchemaException e "Error during reading archive entry %s" entry.FullName

        leftSize <- leftSize - stream.BytesRead
        ret

    let deserializeEntry (entry: ZipArchiveEntry) : 'a =
        readEntry entry
        <| fun reader ->
            try
                downcast myYamlDeserializer.Deserialize(reader, typeof<'a>)
            with :? JsonSerializationException as e ->
                raisefUserWithInner
                    RestoreSchemaException
                    e
                    "Error during deserializing archive entry %s"
                    entry.FullName

    let mutable encounteredSettings: Map<SchemaName, PrettySchemaSettings> = Map.empty

    let mutable encounteredActions: Map<ActionRef, PrettyActionMeta * string> =
        Map.empty

    let mutable encounteredModules: Map<ModuleRef, PrettyModuleMeta * string> =
        Map.empty

    let mutable encounteredTriggers: Map<TriggerRef, PrettyTriggerMeta option * string> =
        Map.empty

    let mutable encounteredUserViews: Map<ResolvedUserViewRef, PrettyUserViewMeta * string> =
        Map.empty

    let mutable encounteredUserViewGeneratorScripts: Map<SchemaName, PrettyUserViewsGeneratorScriptMeta * string> =
        Map.empty

    let parseZipEntry (entry: ZipArchiveEntry) : SchemaName * SchemaDump =
        let (schemaName, rawPath) =
            match entry.FullName with
            | CIRegex @"^([^/]+)/(.*)$" [ rawSchemaName; rawPath ] -> (OzmaQLName rawSchemaName, rawPath)
            | fileName -> raisef RestoreSchemaException "Invalid archive entry %s" fileName

        let dump =
            if entry.Name = "" && entry.Length = 0L then
                // Directory
                emptySchemaDump
            else
                match rawPath with
                | "schema.yaml" ->
                    let settings: PrettySchemaSettings = deserializeEntry entry
                    encounteredSettings <- Map.add schemaName settings encounteredSettings
                    emptySchemaDump
                | CIRegex @"^entities/([^/]+)\.yaml$" [ rawName ] ->
                    let name = OzmaQLName rawName
                    let prettyEntity: PrettyEntity = deserializeEntry entry
                    let (maybeEntityAttrs, entity) = deprettifyEntity prettyEntity

                    { emptySchemaDump with
                        Entities = Map.singleton name entity
                        DefaultAttributes =
                            match maybeEntityAttrs with
                            | Some attrs -> Map.singleton schemaName { Entities = Map.singleton name attrs }
                            | None -> Map.empty }
                | CIRegex @"^triggers/([^/]+)/([^/]+)/([^/]+)\.yaml$" [ rawSchemaName; rawEntityName; rawTriggerName ] ->
                    let ref =
                        { Schema = schemaName
                          Entity =
                            { Schema = OzmaQLName rawSchemaName
                              Name = OzmaQLName rawEntityName }
                          Name = OzmaQLName rawTriggerName }

                    let prettyTriggerMeta: PrettyTriggerMeta = deserializeEntry entry

                    let (prevMeta, prevProc) =
                        Map.findWithLazyDefault ref (fun () -> (None, "")) encounteredTriggers

                    assert (Option.isNone prevMeta)
                    encounteredTriggers <- Map.add ref (Some prettyTriggerMeta, prevProc) encounteredTriggers
                    emptySchemaDump
                | CIRegex @"^triggers/([^/]+)/([^/]+)/([^/]+)\.mjs$" [ rawSchemaName; rawEntityName; rawTriggerName ] ->
                    let ref =
                        { Schema = schemaName
                          Entity =
                            { Schema = OzmaQLName rawSchemaName
                              Name = OzmaQLName rawEntityName }
                          Name = OzmaQLName rawTriggerName }

                    let rawProcedure = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevProc) =
                        Map.findWithLazyDefault ref (fun () -> (None, "")) encounteredTriggers

                    encounteredTriggers <- Map.add ref (prevMeta, rawProcedure) encounteredTriggers
                    emptySchemaDump
                | CIRegex @"^actions/([^/]+)\.yaml$" [ rawActionName ] ->
                    let ref =
                        { Schema = schemaName
                          Name = OzmaQLName rawActionName }

                    let prettyActionMeta: PrettyActionMeta = deserializeEntry entry

                    let (prevMeta, prevFunc) =
                        Map.findWithLazyDefault ref (fun () -> (emptyPrettyActionMeta, "")) encounteredActions

                    encounteredActions <- Map.add ref (prettyActionMeta, prevFunc) encounteredActions
                    emptySchemaDump
                | CIRegex @"^actions/([^/]+)\.mjs$" [ rawActionName ] ->
                    let ref =
                        { Schema = schemaName
                          Name = OzmaQLName rawActionName }

                    let rawFunc = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevFunc) =
                        Map.findWithLazyDefault ref (fun () -> (emptyPrettyActionMeta, "")) encounteredActions

                    encounteredActions <- Map.add ref (prevMeta, rawFunc) encounteredActions
                    emptySchemaDump
                | CIRegex @"^modules/(.*)\.yaml$" [ rawModuleName ] ->
                    let ref =
                        { Schema = schemaName
                          Path = rawModuleName + ".mjs" }

                    let prettyModuleMeta: PrettyModuleMeta = deserializeEntry entry

                    let (prevMeta, prevFunc) =
                        Map.findWithLazyDefault ref (fun () -> (emptyModuleActionMeta, "")) encounteredModules

                    encounteredModules <- Map.add ref (prettyModuleMeta, prevFunc) encounteredModules
                    emptySchemaDump
                | CIRegex @"^modules/(.*\.mjs)$" [ rawModuleName ] ->
                    let ref =
                        { Schema = schemaName
                          Path = rawModuleName }

                    let rawModule = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevFunc) =
                        Map.findWithLazyDefault ref (fun () -> (emptyModuleActionMeta, "")) encounteredModules

                    encounteredModules <- Map.add ref (prevMeta, rawModule) encounteredModules
                    emptySchemaDump
                | CIRegex @"^roles/([^/]+)\.yaml$" [ rawName ] ->
                    let name = OzmaQLName rawName
                    let role: SourceRole = deserializeEntry entry

                    { emptySchemaDump with
                        Roles = Map.singleton name role }
                | CIRegex @"^user_views/([^/]+)\.yaml$" [ rawName ] ->
                    let ref =
                        { Schema = schemaName
                          Name = OzmaQLName rawName }

                    let prettyUvMeta: PrettyUserViewMeta = deserializeEntry entry

                    let (prevMeta, prevUv) =
                        Map.findWithLazyDefault ref (fun () -> (emptyPrettyUserViewMeta, "")) encounteredUserViews

                    encounteredUserViews <- Map.add ref (prettyUvMeta, prevUv) encounteredUserViews
                    emptySchemaDump
                | CIRegex @"^user_views/([^/]+)\.funql$" [ rawName ] ->
                    let ref =
                        { Schema = schemaName
                          Name = OzmaQLName rawName }

                    let rawUv = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevUv) =
                        Map.findWithLazyDefault ref (fun () -> (emptyPrettyUserViewMeta, "")) encounteredUserViews

                    encounteredUserViews <- Map.add ref (prevMeta, rawUv) encounteredUserViews
                    emptySchemaDump
                | CIRegex @"^custom/([^/]+)/([^/]+)\.yaml$" [ rawCustomSchemaName; rawCustomName ] ->
                    let entries = deserializeEntry entry

                    { emptySchemaDump with
                        CustomEntities =
                            Map.singleton
                                (OzmaQLName rawCustomSchemaName)
                                (Map.singleton (OzmaQLName rawCustomName) entries) }
                | fileName when fileName = extraDefaultAttributesEntry ->
                    let defaultAttrs: Map<SchemaName, SourceAttributesSchema> = deserializeEntry entry

                    { emptySchemaDump with
                        DefaultAttributes = defaultAttrs }
                | fileName when fileName = userViewsGeneratorMetaEntry ->
                    let prettyGeneratorMeta: PrettyUserViewsGeneratorScriptMeta = deserializeEntry entry

                    let (prevMeta, prevScript) =
                        Map.findWithLazyDefault
                            schemaName
                            (fun () -> (emptyPrettyUserViewsGeneratorScriptMeta, ""))
                            encounteredUserViewGeneratorScripts

                    encounteredUserViewGeneratorScripts <-
                        Map.add schemaName (prettyGeneratorMeta, prevScript) encounteredUserViewGeneratorScripts

                    emptySchemaDump
                | fileName when fileName = userViewsGeneratorEntry ->
                    let script = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevScript) =
                        Map.findWithLazyDefault
                            schemaName
                            (fun () -> (emptyPrettyUserViewsGeneratorScriptMeta, ""))
                            encounteredUserViewGeneratorScripts

                    encounteredUserViewGeneratorScripts <-
                        Map.add schemaName (prevMeta, script) encounteredUserViewGeneratorScripts

                    emptySchemaDump
                | fileName -> raisef RestoreSchemaException "Invalid archive entry %O/%s" schemaName fileName

        (schemaName, dump)

    let dump =
        zip.Entries
        |> Seq.map (parseZipEntry >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) Map.empty

    let convertAction (KeyValue(ref: ActionRef, (meta: PrettyActionMeta, source))) =
        let ret =
            { emptySchemaDump with
                Actions =
                    Map.singleton
                        ref.Name
                        { Function = source
                          AllowBroken = meta.AllowBroken } }

        (ref.Schema, ret)

    let dump =
        encounteredActions
        |> Seq.map (convertAction >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) dump

    let convertModule (KeyValue(ref: ModuleRef, (meta: PrettyModuleMeta, source))) =
        let ret =
            { emptySchemaDump with
                Modules =
                    Map.singleton
                        ref.Path
                        { Source = source
                          AllowBroken = meta.AllowBroken } }

        (ref.Schema, ret)

    let dump =
        encounteredModules
        |> Seq.map (convertModule >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) dump

    let convertTrigger (KeyValue(ref: TriggerRef, data)) =
        match data with
        | (Some meta, proc) ->
            let entityTriggers =
                { Triggers = Map.singleton ref.Name (deprettifyTrigger meta proc) }

            let schemaTriggers = { Entities = Map.singleton ref.Entity.Name entityTriggers }

            let ret =
                { emptySchemaDump with
                    Triggers = Map.singleton ref.Entity.Schema schemaTriggers }

            (ref.Schema, ret)
        | (None, _) -> raisef RestoreSchemaException "No meta description for trigger %O" ref

    let dump =
        encounteredTriggers
        |> Seq.map (convertTrigger >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) dump

    let convertUserView (KeyValue(ref: ResolvedUserViewRef, (meta: PrettyUserViewMeta, uv))) =
        let ret =
            { emptySchemaDump with
                UserViews =
                    Map.singleton
                        ref.Name
                        { Query = uv
                          AllowBroken = meta.AllowBroken } }

        (ref.Schema, ret)

    let dump =
        encounteredUserViews
        |> Seq.map (convertUserView >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) dump

    let convertUserViewsGeneratorScript (KeyValue(schemaName, (meta: PrettyUserViewsGeneratorScriptMeta, script))) =
        let ret =
            { emptySchemaDump with
                UserViewsGeneratorScript =
                    Some
                        { Script = script
                          AllowBroken = meta.AllowBroken } }

        (schemaName, ret)

    let dump =
        encounteredUserViewGeneratorScripts
        |> Seq.map (convertUserViewsGeneratorScript >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith mergeSchemaDump) dump

    let convertSchema (schemaName: SchemaName) (savedSchema: SchemaDump) =
        let settings =
            Map.findWithDefault schemaName emptyPrettySchemaSettings encounteredSettings

        if settings.OnlyCustomEntities then
            if not <| schemaDumpHasNoSchema savedSchema then
                raisef RestoreSchemaException "Schema %O should only contain custom entities" schemaName

            SSCustomEntities savedSchema.CustomEntities
        else
            SSFull savedSchema

    Map.map convertSchema dump
