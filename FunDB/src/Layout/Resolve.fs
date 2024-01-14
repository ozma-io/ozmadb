module FunWithFlags.FunDB.Layout.Resolve

open System.Security.Cryptography
open System.Text
open FSharpPlus

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.UsedReferences
open FunWithFlags.FunDB.FunQL.Typecheck
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Objects.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type ResolveLayoutException (message : string, innerException : exn, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : exn) =
        ResolveLayoutException (message, innerException, isUserException innerException)

    new (message : string) = ResolveLayoutException (message, null, true)

let private checkName (FunQLName name) : unit =
    if not (goodName name) || String.length name > SQL.sqlIdentifierLength then
        raisef ResolveLayoutException "Invalid name"

let private checkSchemaName (FunQLName str as name) : unit =
    if str.StartsWith("pg_") || str = "information_schema" then
        raisef ResolveLayoutException "Invalid schema name"
    else checkName name

let private checkFieldName (name : FunQLName) : unit =
    if name = funId || name = funSubEntity
    then raisef ResolveLayoutException "Name is forbidden"
    else checkName name

let private reduceDefaultExpr : ParsedFieldExpr -> FieldValue option = function
    | FEValue value -> Some value
    | FECast (FEValue value, typ) ->
        match (value, typ) with
        | (FString s, FTScalar SFTDate) -> Option.map FDate <| SQL.trySqlDate s
        | (FString s, FTScalar SFTDateTime) -> Option.map FDateTime <| SQL.trySqlDateTime s
        | (FString s, FTScalar SFTInterval) -> Option.map FInterval <| SQL.trySqlInterval s
        | (FStringArray vals, FTArray SFTDate) -> Option.map (FDateArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlDate vals)
        | (FStringArray vals, FTArray SFTDateTime) -> Option.map (FDateTimeArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlDateTime vals)
        | (FStringArray vals, FTArray SFTInterval) -> Option.map (FIntervalArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlInterval vals)
        | _ -> None
    | _ -> None

let private hashNameSuffixLength = 4

let private hashNameLength = 20

let private makeHashNameFor (maxLength : int) name =
    if String.length name <= maxLength then
        name
    else
        assert (maxLength >= hashNameSuffixLength)
        use md5 = MD5.Create ()
        let md5Bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(name))
        let stringBuilder = StringBuilder ()
        ignore <| stringBuilder.Append(String.truncate (maxLength - hashNameSuffixLength) name)
        for i = 1 to hashNameSuffixLength / 2 do
            ignore <| stringBuilder.Append(md5Bytes.[i - 1].ToString("x2"))
        string stringBuilder

let private makeHashName (FunQLName name) = makeHashNameFor hashNameLength name

[<NoEquality; NoComparison>]
type private HalfVirtualField =
    { InheritedFrom : ResolvedEntityRef option
    } with
        interface IVirtualFieldBits with
            member this.InheritedFrom = this.InheritedFrom

[<NoEquality; NoComparison>]
type private HalfResolvedComputedField =
    { Source : SourceComputedField
      ColumnName : SQL.ColumnName
      HashName : HashName
      InheritedFrom : ResolvedEntityRef option
      Virtual : HalfVirtualField option
    } with
        interface IComputedFieldBits with
            member this.ColumnName = this.ColumnName
            member this.IsMaterialized = this.Source.IsMaterialized
            member this.AllowBroken = this.Source.AllowBroken
            member this.InheritedFrom = this.InheritedFrom
            member this.Virtual = Option.map (fun x -> upcast x) this.Virtual

[<NoEquality; NoComparison>]
type private HalfResolvedEntity =
    { ColumnFields : Map<FieldName, ResolvedColumnField>
      ComputedFields : Map<FieldName, HalfResolvedComputedField>
      UniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      Children : Map<ResolvedEntityRef, ChildEntity>
      InsertedInternally : bool
      UpdatedInternally : bool
      DeletedInternally : bool
      SaveRestoreKey : ConstraintName option
      TypeName : string
      Root : ResolvedEntityRef
      MainField : FieldName
      HashName : HashName
      Source : SourceEntity
    } with
        // Half-dummy interface for `hasSubType` and maybe other utility functions to work.
        // Doesn't expose computed fields!
        member private this.FindComputedField name =
            Option.map (fun x -> x :> IComputedFieldBits) <| Map.tryFind name this.ComputedFields

        interface IEntityBits with
            member this.FindField name =
                genericFindField (fun name -> Map.tryFind name this.ColumnFields) (fun _ -> None) this name
            member this.FindFieldBits name =
                genericFindField (fun name -> Map.tryFind name this.ColumnFields) this.FindComputedField this name
            member this.Fields =
                let id = Seq.singleton (funId, RId)
                let subentity =
                    if hasSubType this then
                        Seq.singleton (funSubEntity, RSubEntity)
                    else
                        Seq.empty
                let columns = this.ColumnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
                Seq.concat [id; subentity; columns]
            member this.ColumnFields = Map.toSeq this.ColumnFields
            member this.MainField = this.MainField
            member this.TypeName = this.TypeName
            member this.IsAbstract = this.Source.IsAbstract
            member this.IsHidden = this.Source.IsHidden
            member this.Parent = this.Source.Parent
            member this.Children = this.Children

type private HalfResolvedEntitiesMap = Map<ResolvedEntityRef, HalfResolvedEntity>

let private resolveUniqueConstraint (columnFields : Map<FieldName, ResolvedColumnField>) (constrName : ConstraintName) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.Columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    try
        Set.ofSeqUnique constr.Columns |> ignore
    with
    | Failure msg -> raisef ResolveLayoutException "Unique constraint field names clash: %s" msg

    for name in constr.Columns do
        match Map.tryFind name columnFields with
        | None ->
            raisef ResolveLayoutException "Unknown column %O in unique constraint" name
        | Some col ->
            if constr.IsAlternateKey && col.IsNullable then
                raisef ResolveLayoutException "Column %O in alternate key can't be nullable" name

    { Columns = constr.Columns
      IsAlternateKey = constr.IsAlternateKey
      InheritedFrom = None
      HashName = makeHashName constrName
    }

let private sqlColumnName (ref : ResolvedEntityRef) (entity : SourceEntity) (fieldName : FieldName) : SQL.SQLName =
    let str =
        match entity.Parent with
        | None -> string fieldName
        | Some p when p.Schema = ref.Schema -> sprintf "%O__%O" ref.Name fieldName
        | Some p -> sprintf "%O__%O__%O" ref.Schema ref.Name fieldName
    SQL.SQLName (makeHashNameFor SQL.sqlIdentifierLength str)

type private ReferencingEntitiesMap = Map<ResolvedEntityRef, Map<ResolvedFieldRef, ReferenceDeleteAction>>

//
// PHASE 1: Building column fields. Also collect several useful maps.
//

type private SchemaObjectNames =
      { ForeignConstraintNames : Map<SQL.SQLName, ResolvedFieldRef>
        UniqueConstraintNames : Map<SQL.SQLName, ResolvedConstraintRef>
        CheckConstraintNames : Map<SQL.SQLName, ResolvedConstraintRef>
        IndexNames : Map<SQL.SQLName, ResolvedIndexRef>
      }

let private emptySchemaObjectNames =
    { ForeignConstraintNames = Map.empty
      UniqueConstraintNames = Map.empty
      CheckConstraintNames = Map.empty
      IndexNames = Map.empty
    }

type private Phase1Resolver (layout : SourceLayout) =
    let mutable cachedEntities : HalfResolvedEntitiesMap = Map.empty
    let mutable referencingFields : ReferencingEntitiesMap = Map.empty
    let mutable objectNames : Map<SchemaName, SchemaObjectNames> = Map.empty
    let mutable rootEntities : Set<ResolvedEntityRef> = Set.empty
    let mutable internallyDeletedEntities : Set<ResolvedEntityRef> = Set.empty
    let mutable cascadeDeletedEntities : Set<ResolvedEntityRef> = Set.empty

    let mapSchemaNames (f : SchemaObjectNames -> SchemaObjectNames) (name : SchemaName) =
        let schemaNames = Map.findWithDefault name emptySchemaObjectNames objectNames
        let schemaNames = f schemaNames
        objectNames <- Map.add name schemaNames objectNames

    let unionComputedField (name : FieldName) (parent : HalfResolvedComputedField) (child : HalfResolvedComputedField) =
        match (parent.Virtual, child.Virtual) with
        | (Some parentVirtual, Some childVirtual) ->
            let newInheritedFrom =
                match parentVirtual.InheritedFrom with
                | Some from -> from
                | None ->
                    // `parent` is root virtual field. When `unionComputedField` is called parent fields already have `InheritedFrom` set, so the following is safe.
                    Option.get parent.InheritedFrom

            { child with
                  Virtual = Some { childVirtual with InheritedFrom = Some newInheritedFrom }
                  // Always use root column name.
                  ColumnName = parent.ColumnName
            }
        | _ ->
            raisef ResolveLayoutException "Computed field names clash: %O" name

    let resolveFieldType (fieldRef : ResolvedFieldRef) (field : SourceColumnField) (ft : ParsedFieldType) : ResolvedFieldType =
        try
            match resolveFieldType layout (Some fieldRef.Entity.Schema) true ft with
            | FTScalar SFTUserViewRef
            | FTArray SFTUserViewRef -> raisef ResolveLayoutException "User view references are not supported as field types"
            | FTArray (SFTReference (ref, opts)) -> raisef ResolveLayoutException "Arrays of references are not supported as field types"
            | FTArray (SFTEnum _) -> raisef ResolveLayoutException "Arrays of enums are not supported as field types"
            | FTScalar (SFTReference (refEntityRef, opts)) as t ->
                let deleteAction = Option.defaultValue RDANoAction opts
                match deleteAction with
                | RDANoAction -> ()
                | RDACascade ->
                    cascadeDeletedEntities <- Set.add fieldRef.Entity cascadeDeletedEntities
                | RDASetNull ->
                    if not field.IsNullable then
                        raisef ResolveLayoutException "Can't declare non-nullable reference field SET NULL"
                    if field.IsImmutable then
                        raisef ResolveLayoutException "Can't declare immutable reference field SET NULL"
                | RDASetDefault ->
                    if Option.isNone field.DefaultValue then
                        raisef ResolveLayoutException "Can't declare reference field with no default value SET DEFAULT"
                    if field.IsImmutable then
                        raisef ResolveLayoutException "Can't declare immutable reference field SET DEFAULT"

                referencingFields <- Map.addWith Map.unionUnique refEntityRef (Map.singleton fieldRef deleteAction) referencingFields

                t
            | t -> t
        with
        | :? QueryResolveException as e -> raisefWithInner ResolveLayoutException e ""

    let resolveColumnField (entityRef : ResolvedEntityRef) (entity : SourceEntity) (fieldName : FieldName) (col : SourceColumnField) : ResolvedColumnField =
        let fieldRef = { Entity = entityRef; Name = fieldName }
        let fieldType =
            match parse tokenizeFunQL fieldType col.Type with
            | Ok r -> resolveFieldType fieldRef col r
            | Error msg -> raisef ResolveLayoutException "Error parsing column field type %s: %s" col.Type msg
        let defaultValue =
            match col.DefaultValue with
            | None -> None
            | Some def ->
                match parse tokenizeFunQL fieldExpr def with
                | Ok r ->
                    match reduceDefaultExpr r with
                    | Some FNull -> raisef ResolveLayoutException "Default expression cannot be NULL"
                    | Some v ->
                        match (fieldType, v) with
                        // DEPRECATED: we allow integers as defaults for references.
                        | (FTScalar (SFTReference (_, _)), FInt _) -> ()
                        | (FTArray (SFTReference (_, _)), FIntArray _) -> ()
                        | _ when (isValueOfSubtype fieldType v) -> ()
                        | _ -> raisef ResolveLayoutException "Default value %O is not of type %O" v fieldType
                        Some v
                    | None -> raisef ResolveLayoutException "Default expression is not trivial: %s" def
                | Error msg -> raisef ResolveLayoutException "Error parsing column field default expression: %s" msg

        { FieldType = fieldType
          ValueType = compileFieldType fieldType
          DefaultValue = defaultValue
          IsNullable = col.IsNullable
          IsImmutable = col.IsImmutable
          InheritedFrom = None
          ColumnName = sqlColumnName entityRef entity fieldName
          HashName = makeHashName fieldName
        }

    let resolveEntity (parent : HalfResolvedEntity option) (entityRef : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        let root =
            match parent with
            | None ->
                rootEntities <- Set.add entityRef rootEntities
                entityRef
            | Some p -> p.Root
        let hashName = makeHashName entityRef.Name

        let mapColumnField name field =
            try
                checkFieldName name
                resolveColumnField entityRef entity name field
            with
            | e -> raisefWithInner ResolveLayoutException e "In column field %O" name
        let mapComputedField name (field : SourceComputedField) =
            try
                checkFieldName name
                { Source = field
                  InheritedFrom = None
                  Virtual = if field.IsVirtual then Some { InheritedFrom = None } else None
                  ColumnName = sqlColumnName entityRef entity name
                  HashName = makeHashName name
                }
            with
            | e -> raisefWithInner ResolveLayoutException e "In computed field %O" name

        let selfColumnFields = Map.map mapColumnField entity.ColumnFields
        let columnFields =
            match parent with
            | None -> selfColumnFields
            | Some p ->
                let parentRef = Option.get entity.Parent
                let addParent (field : ResolvedColumnField) =
                    if Option.isNone field.InheritedFrom then
                        { field with InheritedFrom = Some parentRef }
                    else
                        field
                let inheritedFields = Map.map (fun name field -> addParent field) p.ColumnFields
                try
                    Map.unionUnique inheritedFields selfColumnFields
                with
                | Failure msg -> raisef ResolveLayoutException "Column field names clash: %s" msg

        let selfComputedFields = Map.map mapComputedField entity.ComputedFields
        let computedFields =
            match parent with
            | None -> selfComputedFields
            | Some p ->
                let parentRef = Option.get entity.Parent
                let addParent = function
                    | None -> Some parentRef
                    | Some _ as pref -> pref
                let inheritedFields = Map.map (fun name field -> { field with InheritedFrom = addParent field.InheritedFrom } : HalfResolvedComputedField) p.ComputedFields
                Map.unionWithKey unionComputedField inheritedFields selfComputedFields

        try
            // At this point we have already checked name clashes through hierarchy, we only need to check that hash names are unique.
            let columnNames = selfColumnFields |> Map.values |> Seq.map (fun field -> field.HashName)
            let computedNames = selfComputedFields |> Map.values |> Seq.map (fun field -> field.HashName)
            Seq.append columnNames computedNames |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Field names hash clash: %s" msg

        let foreignConstraintNames =
            let makeName (name, c : ResolvedColumnField) =
                match c.FieldType with
                | FTScalar (SFTReference (e, _))
                | FTArray (SFTReference (e, _)) ->
                    Some (foreignConstraintSQLName hashName c.HashName, { Entity = entityRef; Name = name })
                | _ -> None
            selfColumnFields |> Map.toList |> Seq.mapMaybe makeName |> Map.ofSeqUnique

        let mapUniqueConstraint name constr =
            try
                checkName name
                resolveUniqueConstraint columnFields name constr
            with
            | e -> raisefWithInner ResolveLayoutException e "In unique constraint %O" name
        let selfUniqueConstraints = Map.map mapUniqueConstraint entity.UniqueConstraints

        let uniqueConstraints =
            match parent with
            | None -> selfUniqueConstraints
            | Some p ->
                let parentRef = Option.get entity.Parent
                let addParent (constr : ResolvedUniqueConstraint) =
                    if Option.isNone constr.InheritedFrom then
                        { constr with InheritedFrom = Some parentRef }
                    else
                        constr
                let inheritedConstrs = Map.map (fun name field -> addParent field) p.UniqueConstraints
                try
                    Map.unionUnique inheritedConstrs selfUniqueConstraints
                with
                | Failure msg -> raisef ResolveLayoutException "Column field names clash: %s" msg

        let uniqueConstraintNames =
            let makeName (name, c : ResolvedUniqueConstraint) = (uniqueConstraintSQLName hashName c.HashName, { Entity = entityRef; Name = name })
            try
                selfUniqueConstraints |> Map.toList |> Seq.map makeName |> Map.ofSeqUnique
            with
            | Failure msg -> raisef ResolveLayoutException "Unique constraint names clash (first %i characters): %s" hashNameLength msg

        let addNames (schemaNames : SchemaObjectNames) =
            { schemaNames with
                UniqueConstraintNames = Map.union schemaNames.UniqueConstraintNames uniqueConstraintNames
                ForeignConstraintNames = Map.union schemaNames.ForeignConstraintNames foreignConstraintNames
            }
        mapSchemaNames addNames root.Schema

        let parentSaveRestoreKey =
            match parent with
            | None -> None
            | Some p -> p.SaveRestoreKey

        let failKey parentKey currKey = raisef ResolveLayoutException "Entity %O overrides save-restore key of parent entity %O, which is forbidden" entityRef (Option.get entity.Parent)
        let saveRestoreKey = Option.unionWith failKey parentSaveRestoreKey entity.SaveRestoreKey

        let typeName =
            if root.Schema = entityRef.Schema then
                entityRef.Name.ToString()
            else
                sprintf "%O__%O" entityRef.Schema entityRef.Name

        let getFlag f = Option.defaultValue false (Option.map f parent)

        let mainField =
            match entity.MainField with
            | Some mainField ->
                match parent with
                | Some p when p.MainField <> mainField ->
                    raisef ResolveLayoutException "Main field %O in parent entity differs from current" p.MainField
                | Some p -> ()
                | None ->
                    if not (mainField = funId
                            || Map.containsKey mainField columnFields
                            || Map.containsKey mainField computedFields) then
                        raisef ResolveLayoutException "Nonexistent or unsupported main field: %O" mainField
                mainField
            | None ->
                match parent with
                | None -> funId
                | Some p -> p.MainField

        let ret =
            { ColumnFields = columnFields
              ComputedFields = computedFields
              UniqueConstraints = uniqueConstraints
              InsertedInternally = getFlag (fun parent -> parent.InsertedInternally) || entity.InsertedInternally
              UpdatedInternally = getFlag (fun parent -> parent.UpdatedInternally) || entity.UpdatedInternally
              DeletedInternally = getFlag (fun parent -> parent.DeletedInternally) || entity.DeletedInternally
              SaveRestoreKey = saveRestoreKey
              Children = Map.empty
              Root = root
              TypeName = typeName
              MainField = mainField
              HashName = hashName
              Source = entity
            }

        if ret.DeletedInternally then
            internallyDeletedEntities <- Set.add entityRef internallyDeletedEntities

        ret

    let rec addIndirectChildren (childRef : ResolvedEntityRef) (entity : HalfResolvedEntity) =
        match entity.Source.Parent with
        | None -> ()
        | Some parentRef ->
            let oldParent = Map.find parentRef cachedEntities
            cachedEntities <- Map.add parentRef { oldParent with Children = Map.add childRef { Direct = false } oldParent.Children } cachedEntities
            addIndirectChildren childRef oldParent

    let rec resolveOneEntity (stack : Set<ResolvedEntityRef>) (child : ResolvedEntityRef option) (ref : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        try
            let resolveParent parentRef =
                let newStack = Set.add ref stack
                if Set.contains parentRef newStack then
                    raisef ResolveLayoutException "Inheritance cycle detected"
                else
                    if parentRef.Schema <> ref.Schema then
                        raisef ResolveLayoutException "Cross-schema inheritance is forbidden" parentRef
                    let parentEntity =
                        match layout.FindEntity parentRef with
                        | None -> raisef ResolveLayoutException "Parent entity %O not found" parentRef
                        | Some parent -> parent
                    resolveOneEntity newStack (Some ref) parentRef parentEntity

            match Map.tryFind ref cachedEntities with
            | Some cached ->
                match child with
                | None -> ()
                | Some childRef ->
                    cachedEntities <- Map.add ref { cached with Children = Map.add childRef { Direct = true } cached.Children } cachedEntities
                    addIndirectChildren childRef cached
                cached
            | None ->
                checkName ref.Name
                let parent = Option.map resolveParent entity.Parent
                let entity = resolveEntity parent ref entity
                let entityWithChild =
                    match child with
                    | None -> entity
                    | Some childRef ->
                        addIndirectChildren childRef entity
                        { entity with Children = Map.singleton childRef { Direct = true } }
                cachedEntities <- Map.add ref entityWithChild cachedEntities
                entity
        with
        | e -> raisefWithInner ResolveLayoutException e "In entity %O" ref

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) =
        let rec iterEntity name entity =
            let ref = { Schema = schemaName; Name = name }
            let entity = resolveOneEntity Set.empty None ref entity
            ()
        schema.Entities |> Map.iter iterEntity

    let resolveLayout () =
        let iterSchema name schema =
            try
                checkSchemaName name
                resolveSchema name schema
            with
            | e -> raisefWithInner ResolveLayoutException e "In schema %O" name
        Map.iter iterSchema layout.Schemas

    member this.ResolveLayout () =
        resolveLayout ()
        cachedEntities

    member this.RootEntities = rootEntities
    member this.ObjectNames = objectNames
    member this.ReferencingFields = referencingFields
    member this.InternallyDeletedEntities = internallyDeletedEntities
    member this.CascadeDeletedEntities = cascadeDeletedEntities

let private fillReferencingFields (rootEntities : ResolvedEntityRef seq) (entities : HalfResolvedEntitiesMap) (initialReferencesMap : ReferencingEntitiesMap) : ReferencingEntitiesMap =
    let mutable referencesMap = initialReferencesMap

    let rec go (prevReferences : Map<ResolvedFieldRef, ReferenceDeleteAction>) (entityRef : ResolvedEntityRef) =
        let entity = Map.find entityRef entities
        let thisReferences = Map.findWithDefault entityRef Map.empty referencesMap
        let references = Map.unionUnique prevReferences thisReferences
        referencesMap <- Map.add entityRef references referencesMap
        for KeyValue(childRef, child) in entity.Children do
            if child.Direct then
                go references childRef

    for root in rootEntities do
        go Map.empty root

    referencesMap

let private fillCascadeDeletedEntities (entities : HalfResolvedEntitiesMap) (initialCascadeDeletedEntities : Set<ResolvedEntityRef>) : Set<ResolvedEntityRef> =
    let mutable cascadeDeletedEntities = Set.empty

    let rec markEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) =
        cascadeDeletedEntities <- Set.add entityRef cascadeDeletedEntities
        match entity.Source.Parent with
        | Some parentRef when not <| Set.contains parentRef cascadeDeletedEntities ->
            let parent = Map.find parentRef entities
            markEntity parentRef parent
        | _ -> ()

    let rec markChildren (entity : HalfResolvedEntity) =
        for KeyValue(childRef, child) in entity.Children do
            if child.Direct then
                cascadeDeletedEntities <- Set.add childRef cascadeDeletedEntities
                let childEntity = Map.find childRef entities
                markChildren childEntity

    for entityRef in initialCascadeDeletedEntities do
        let entity = Map.find entityRef entities
        markEntity entityRef entity
        markChildren entity

    cascadeDeletedEntities

let private parseRelatedExpr (rawExpr : string) =
    match parse tokenizeFunQL fieldExpr rawExpr with
    | Ok r -> r
    | Error msg -> raisef ResolveLayoutException "Error parsing local expression: %s" msg

let private relatedResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

//
// PHASE 2: Building computed fields, sub entity parse expressions and other stuff.
//

let rec private sortSaveRestoredEntities' (visited : Set<ResolvedEntityRef>) (saveRestoredParents : (ResolvedEntityRef * ResolvedEntityRef) seq) =
    if Seq.isEmpty saveRestoredParents then
        Seq.empty
    else
        let (currentGen, nextGen) = Seq.partition (fun (ref, parentRef) -> Set.contains parentRef visited) saveRestoredParents
        let currSet = Seq.map fst currentGen |> Set.ofSeq
        assert (not <| Set.isEmpty currSet)
        seq {
            yield! currSet
            yield! sortSaveRestoredEntities' (Set.union visited currSet) nextGen
        }

let private sortSaveRestoredEntities (saveRestoredParents : (ResolvedEntityRef * ResolvedEntityRef option) seq) =
    let (rootGen, nextGen) = Seq.partition (fun (ref, maybeParentRef) -> Option.isNone maybeParentRef) saveRestoredParents
    let rootSet = Seq.map fst rootGen |> Set.ofSeq
    seq {
        yield! rootSet
        yield! sortSaveRestoredEntities' rootSet (Seq.map (fun (ref, parentRef) -> (ref, Option.get parentRef)) nextGen)
    }

type private Phase2Resolver (
        layout : SourceLayout,
        entities : HalfResolvedEntitiesMap,
        halfObjectNames : Map<SchemaName, SchemaObjectNames>,
        referencingFields : ReferencingEntitiesMap,
        cascadeDeletedEntities : Set<ResolvedEntityRef>,
        forceAllowBroken : bool
    ) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, PossiblyBroken<ResolvedComputedField>> = Map.empty
    let mutable cachedCaseExpressions : Map<ResolvedFieldRef, VirtualFieldCase array> = Map.empty
    let mutable cachedSaveRestoredEntities : Map<ResolvedEntityRef, ResolvedEntityRef option> = Map.empty
    let mutable objectNames = halfObjectNames

    let mapSchemaNames (f : SchemaObjectNames -> SchemaObjectNames) (name : SchemaName) =
        let schemaNames = Map.find name objectNames
        let schemaNames = f schemaNames
        objectNames <- Map.add name schemaNames objectNames

    let rec makeWrappedLayout (stack : Set<ResolvedFieldRef>) : ILayoutBits =
        // We "lazily" instance wrapped entities, which in turn lazily instance computed fields.
        // All this to support using computed fields in computed fields, and check them for cycles while doing that.
        let wrapEntity (currRef : ResolvedEntityRef) (currEnt : HalfResolvedEntity) =
            let findComputedField name =
                match Map.tryFind name currEnt.ComputedFields with
                | None -> None
                | Some comp ->
                    let fieldRef = { Entity = currRef; Name = name }
                    match resolveComputedField stack currEnt fieldRef comp with
                    | Ok field -> Some field
                    | Error e -> raisefWithInner ResolveLayoutException e.Error "Computed field %O is broken" fieldRef
            let findComputedFieldBits name =
                Option.map (fun x -> x :> IComputedFieldBits) <| Map.tryFind name currEnt.ComputedFields

            { new IEntityBits with
                member this.FindField name =
                    genericFindField (fun name -> Map.tryFind name currEnt.ColumnFields) findComputedField this name
                member this.FindFieldBits name =
                    genericFindField (fun name -> Map.tryFind name currEnt.ColumnFields) findComputedFieldBits this name
                member this.Fields =
                    let id = Seq.singleton (funId, RId)
                    let subentity =
                        if hasSubType this then
                            Seq.singleton (funSubEntity, RSubEntity)
                        else
                            Seq.empty
                    let colFields = currEnt.ColumnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, RColumnField col))
                    let compFields = currEnt.ComputedFields |> Map.toSeq |> Seq.map (fun (name, comp) -> (name, RComputedField (comp :> IComputedFieldBits)))
                    Seq.concat [id; subentity; colFields; compFields]
                member this.ColumnFields = Map.toSeq currEnt.ColumnFields
                member this.MainField = currEnt.MainField
                member this.TypeName = currEnt.TypeName
                member this.IsAbstract = currEnt.Source.IsAbstract
                member this.IsHidden = currEnt.Source.IsHidden
                member this.Parent = currEnt.Source.Parent
                member this.Children = currEnt.Children
            }

        let mutable cachedEntities = Map.empty
        { new ILayoutBits with
            member this.HasVisibleEntity ref =
                match Map.tryFind ref entities with
                | Some entity when not entity.Source.IsHidden -> true
                | _ -> false
            member this.FindEntity ref =
                match Map.tryFind ref cachedEntities with
                | Some cached -> Some cached
                | None ->
                    match Map.tryFind ref entities with
                    | None -> None
                    | Some entity ->
                        let wrappedEntity = wrapEntity ref entity
                        cachedEntities <- Map.add ref wrappedEntity cachedEntities
                        Some wrappedEntity
        }

    and initialWrappedLayout = makeWrappedLayout Set.empty

    and resolveRelatedExpr (wrappedLayout : ILayoutBits) (entityRef : ResolvedEntityRef) (expr : ParsedFieldExpr) : SingleFieldExprInfo * ResolvedFieldExpr =
        let entityInfo = SFEntity (customEntityMapping entityRef)
        let callbacks =
            { resolveCallbacks wrappedLayout with
                HomeSchema = Some entityRef.Schema
            }
        let (exprInfo, expr) =
            try
                resolveSingleFieldExpr callbacks OrderedMap.empty relatedResolutionFlags entityInfo expr
            with
            | :? QueryResolveException as e -> raisefWithInner ResolveLayoutException e ""
        if exprInfo.Flags.HasAggregates then
            raisef ResolveLayoutException "Aggregate functions are not allowed here"
        (exprInfo, expr)

    and resolveMyVirtualCases (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) : VirtualFieldCase array =
        let childVirtualCases = findVirtualChildren entity fieldRef.Name |> Seq.toArray
        let handledChildren = childVirtualCases |> Seq.map (fun c -> c.PossibleEntities) |> Set.unionMany

        let isMyselfHandled (ref, entity : HalfResolvedEntity) =
            not (entity.Source.IsAbstract || Set.contains ref handledChildren)

        let childrenEntities = entity.Children |> Map.keys |> Seq.map (fun ref -> (ref, Map.find ref entities))
        let allEntities = Seq.append (Seq.singleton (fieldRef.Entity, entity)) childrenEntities
        let myCases = allEntities |> Seq.filter isMyselfHandled |> Seq.map fst |> Set.ofSeq

        if Set.isEmpty myCases then
            childVirtualCases
        else
            let myCase =
                { Ref = fieldRef.Entity
                  PossibleEntities = myCases
                }
            Array.append childVirtualCases [|myCase|]

    and resolveVirtualCases (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) : VirtualFieldCase[] =
        match Map.tryFind fieldRef cachedCaseExpressions with
        | Some cached -> cached
        | None ->
            let cases = resolveMyVirtualCases entity fieldRef
            cachedCaseExpressions <- Map.add fieldRef cases cachedCaseExpressions
            cases

    // We perform a DFS here, ordering virtual fields children-first.
    and findVirtualChildren (entity : HalfResolvedEntity) (fieldName : FieldName) : VirtualFieldCase seq =
        entity.Children |> Map.toSeq |> Seq.collect (fun (ref, child) -> findVirtualChild fieldName ref child)

    and findVirtualChild (fieldName : FieldName) (entityRef : ResolvedEntityRef) (child : ChildEntity) : VirtualFieldCase seq =
        if not child.Direct then
            Seq.empty
        else
            let childEntity = Map.find entityRef entities
            let childField = Map.find fieldName childEntity.ComputedFields
            match childField with
            | comp when Option.isNone comp.InheritedFrom ->
                resolveVirtualCases childEntity { Entity = entityRef; Name = fieldName } |> Array.toSeq
            | _ -> findVirtualChildren childEntity fieldName

    and resolveOneComputeField (stack : Set<ResolvedFieldRef>) (fieldRef : ResolvedFieldRef) (entity : HalfResolvedEntity) (comp : HalfResolvedComputedField) : ResolvedComputedField =
        let wrappedLayout = makeWrappedLayout stack
        // Computed fields are always assumed to reference immediate fields. Also read shortcomings of current computed field compilation in Compile.fs.
        let (exprInfo, expr) = resolveRelatedExpr wrappedLayout fieldRef.Entity (parseRelatedExpr comp.Source.Expression)
        let usedReferences = fieldExprUsedReferences wrappedLayout expr

        if comp.Source.IsMaterialized then
            if exprInfo.Flags.HasSubqueries then
                raisef ResolveLayoutException "Queries are not supported in materialized computed fields"
            if exprInfo.Flags.HasArguments then
                raisef ResolveLayoutException "Arguments are not allowed in materialized computed fields"
            if usedReferences.HasRestrictedEntities then
                raisef ResolveLayoutException "Restricted (plain) join arrows are not allowed in materialized computed fields"
        let maybeExprType =
            try
                typecheckFieldExpr wrappedLayout expr
            with
            | :? ViewTypecheckException as e -> raisefWithInner ResolveLayoutException e "Failed to typecheck computed field"
        let exprType =
            Option.getOrFailWith (fun () -> raisef ResolveLayoutException "Computed field type cannot be inferred") maybeExprType
        let virtualInfo =
            match comp.Virtual with
            | None -> None
            | Some v ->
                Some
                    { Cases = resolveVirtualCases entity fieldRef
                      InheritedFrom = v.InheritedFrom
                    }

        { Expression = expr
          Type = exprType
          InheritedFrom = None
          Flags = exprInfo.Flags
          UsedDatabase = usedReferences.UsedDatabase
          AllowBroken = comp.Source.AllowBroken
          HashName = comp.HashName
          ColumnName = comp.ColumnName
          Virtual = virtualInfo
          Root = None
          IsMaterialized = comp.Source.IsMaterialized
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : HalfResolvedComputedField) : PossiblyBroken<ResolvedComputedField> =
        match comp.InheritedFrom with
        | Some parentRef ->
            let origFieldRef = { fieldRef with Entity = parentRef }
            let origEntity = Map.find parentRef entities
            resolveComputedField stack origEntity origFieldRef (Map.find fieldRef.Name origEntity.ComputedFields)
                |> Result.map (fun f -> { f with InheritedFrom = Some parentRef })
        | None ->
            match Map.tryFind fieldRef cachedComputedFields with
            | Some f -> f
            | None ->
                if Set.contains fieldRef stack then
                    raisef ResolveLayoutException "Cycle detected in computed fields: %O" stack
                let newStack = Set.add fieldRef stack
                try
                    let field = Ok <| resolveOneComputeField newStack fieldRef entity comp
                    cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                    field
                with
                | :? ResolveLayoutException as e when comp.Source.AllowBroken || forceAllowBroken ->
                    Error { Error = e; AllowBroken = comp.Source.AllowBroken }

    and resolveLocalExpr (entityRef : ResolvedEntityRef) (expr : ParsedFieldExpr) : SingleFieldExprInfo * ResolvedFieldExpr =
        // Local expressions are used as-is in the database, so `Immediate = false`.
        let (exprInfo, expr) = resolveRelatedExpr initialWrappedLayout entityRef expr
        if not <| exprIsLocal exprInfo.Flags then
            raisef ResolveLayoutException "Non-local expressions are not allowed here"
        if exprInfo.Flags.HasArguments then
            raisef ResolveLayoutException "Arguments are not allowed here"
        (exprInfo, expr)

    let resolveCheckConstraint (entityRef : ResolvedEntityRef) (constrName : ConstraintName) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let (exprInfo, expr) = resolveRelatedExpr initialWrappedLayout entityRef (parseRelatedExpr constr.Expression)
        if exprInfo.Flags.HasSubqueries then
            raisef ResolveLayoutException "Subqueries are not allowed here"
        if exprInfo.Flags.HasArguments then
            raisef ResolveLayoutException "Arguments are not allowed here"
        let usedReferences = fieldExprUsedReferences initialWrappedLayout expr
        { Expression = expr
          UsedDatabase = usedReferences.UsedDatabase
          IsLocal = exprIsLocal exprInfo.Flags
          HashName = makeHashName constrName
        }

    let resolveIndexColumn (entityRef : ResolvedEntityRef) (indexType : IndexType) (colExpr : string) : ResolvedIndexColumn =
        let col =
            match parse tokenizeFunQL indexColumn colExpr with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing index expression: %s" msg

        let (exprInfo, expr) = resolveLocalExpr entityRef col.Expr
        let indexInfo = getIndexTypeInfo indexType
        let (order, nullsOrder) =
            if indexInfo.CanOrder then
                let order = Option.defaultValue Asc col.Order
                let nullsOrder =
                    match col.Nulls with
                    | Some n -> n
                    | None ->
                        match order with
                        | Asc -> NullsLast
                        | Desc -> NullsFirst
                (Some order, Some nullsOrder)
            else
                if Option.isSome col.Order || Option.isSome col.Nulls then
                    raisef ResolveLayoutException "Cannot specify ordering for unordered index"
                (None, None)
        match col.OpClass with
        | None -> ()
        | Some cl ->
            match Map.tryFind cl allowedOpClasses with
            | None -> raisef ResolveLayoutException "Invalid operation class: %O" cl
            | Some clOps ->
                if not <| Map.containsKey indexType clOps then
                    raisef ResolveLayoutException "Invalid operation class for index type %O: %O" indexType cl
        { Expr = expr
          OpClass = col.OpClass
          Order = order
          Nulls = nullsOrder
        }

    let resolveIndex (entityRef : ResolvedEntityRef) (indexName : IndexName) (index : SourceIndex) : ResolvedIndex =
        if Array.isEmpty index.Expressions then
            raise <| ResolveLayoutException "Empty index"

        let exprs = Array.map (resolveIndexColumn entityRef index.Type) index.Expressions
        let includedExprs = Array.map (fun x -> resolveLocalExpr entityRef (parseRelatedExpr x) |> snd) index.IncludedExpressions
        let predicate = Option.map (fun x -> resolveLocalExpr entityRef (parseRelatedExpr x) |> snd) index.Predicate

        { Expressions = exprs
          IncludedExpressions = includedExprs
          HashName = makeHashName indexName
          IsUnique = index.IsUnique
          Predicate = predicate
          Type = index.Type
        }

    let rec checkSaveRestoreKey (visited : Set<ResolvedEntityRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (saveRestoreKey : ConstraintName) =
        if Map.containsKey entityRef cachedSaveRestoredEntities then
            ()
        else
            let visited = Set.add entityRef visited
            let constr =
                match Map.tryFind saveRestoreKey entity.UniqueConstraints with
                | None -> raisef ResolveLayoutException "Alternate key %O specified as save-restore key not found" saveRestoreKey
                | Some constr when not constr.IsAlternateKey -> raisef ResolveLayoutException "Unique constraint %O specified as save-restore key is not an alternate key" saveRestoreKey
                | Some constr -> constr

            // First field in save-restore key should either be:
            // * `public.schemas.name` (schema name);
            // * A reference to a saved-restored entity.
            let firstColumn = constr.Columns.[0]
            let parent =
                if entityRef = schemasNameFieldRef.Entity && firstColumn = schemasNameFieldRef.Name then
                    None
                else
                    let firstField = Map.find firstColumn entity.ColumnFields
                    match firstField.FieldType with
                    | FTScalar (SFTReference (refEntityRef, opts)) -> Some refEntityRef
                    | _ -> None

            for KeyValue(fieldName, field) in entity.ColumnFields do
                let keyColumn = Array.contains fieldName constr.Columns
                ignore <| checkSaveRestoreField keyColumn visited { Entity = entityRef; Name = fieldName } field

            for KeyValue(childRef, child) in entity.Children do
                let childEntity = Map.find childRef entities
                if Option.isSome childEntity.SaveRestoreKey then
                    raisef ResolveLayoutException "Child entity %O can't have save-restore key declared, as it conflicts with definition in parent entity %O" childRef entityRef
                for KeyValue(fieldName, field) in childEntity.ColumnFields do
                    if Option.isNone field.InheritedFrom then
                        ignore <| checkSaveRestoreField true visited { Entity = childRef; Name = fieldName } field

            cachedSaveRestoredEntities <- Map.add entityRef parent cachedSaveRestoredEntities

    and checkSaveRestoreField (keyColumn : bool) (visited : Set<ResolvedEntityRef>) (fieldRef : ResolvedFieldRef) (field : ResolvedColumnField) =
        match field.FieldType with
        | FTScalar (SFTReference (refEntityRef, opts)) ->
            if Set.contains refEntityRef visited then
                if keyColumn then
                    raisef ResolveLayoutException "Recursive reference to %O in save-restore key column %O is forbidden" refEntityRef fieldRef
                // We don't support recursion in custom save-restored entity values for now.
                else if fieldRef.Entity.Schema <> funSchema then
                    raisef ResolveLayoutException "Recursive reference to %O in save-restored column %O is not yet supported" refEntityRef fieldRef
            else
                let refEntity = Map.find refEntityRef entities
                match refEntity.SaveRestoreKey with
                | None -> raisef ResolveLayoutException "Entity %O referenced in %O is not save-restored" refEntityRef fieldRef
                | Some refKey -> checkSaveRestoreKey visited refEntityRef refEntity refKey
        | FTScalar SFTInt
        | FTScalar SFTString
        | FTScalar SFTBool
        | FTScalar SFTUuid
        | FTScalar (SFTEnum _) -> ()
        | typ when keyColumn -> raisef ResolveLayoutException "Type %O of field %O is not supported for key columns" typ fieldRef
        | _ -> ()

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ResolvedEntity =
        let mapComputedField name field =
            try
                resolveComputedField Set.empty entity { Entity = entityRef; Name = name } field
            with
            | e -> raisefWithInner ResolveLayoutException e "In computed field %O" name

        let computedFields = Map.map mapComputedField entity.ComputedFields

        // Ideally we would use `tempEntity` for it too, but the code for `resolveRelatedExpr` is already here, so...
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint entityRef name constr
            with
            | e -> raisefWithInner ResolveLayoutException e "In check constraint %O" name
        let checkConstraints = Map.map mapCheckConstraint entity.Source.CheckConstraints

        let checkConstraintNames =
            let makeName (name, c : ResolvedCheckConstraint) = (checkConstraintSQLName entity.HashName c.HashName, { Entity = entityRef; Name = name })
            try
                checkConstraints |> Map.toSeq |> Seq.map makeName |> Map.ofSeqUnique
            with
            | Failure msg -> raisef ResolveLayoutException "Check constraint names clash (first %i characters): %s" hashNameLength msg

        let mapIndex name index =
            try
                checkName name
                resolveIndex entityRef name index
            with
            | e -> raisefWithInner ResolveLayoutException e "In index %O" name
        let indexes = Map.map mapIndex entity.Source.Indexes

        let indexNames =
            let makeName (name, i : ResolvedIndex) = (indexSQLName entity.HashName i.HashName, { Entity = entityRef; Name = name })
            try
                indexes |> Map.toSeq |> Seq.map makeName |> Map.ofSeqUnique
            with
            | Failure msg -> raisef ResolveLayoutException "Index names clash (first %i characters): %s" hashNameLength msg

        let wrappedEntity = initialWrappedLayout.FindEntity entityRef |> Option.get
        // Check that main field is valid.
        wrappedEntity.FindField funMain |> Option.get |> ignore

        let requiredFields =
            entity.ColumnFields
            |> Map.toSeq
            |> Seq.mapMaybe (fun (fieldName, field) -> if fieldIsOptional field then None else Some fieldName)
            |> Set.ofSeq

        match entity.Source.SaveRestoreKey with
        | None -> ()
        | Some saveRestoreKey -> checkSaveRestoreKey Set.empty entityRef entity saveRestoreKey

        let addNames (schemaNames : SchemaObjectNames) =
            { schemaNames with
                CheckConstraintNames = Map.union schemaNames.CheckConstraintNames checkConstraintNames
                IndexNames = Map.union schemaNames.IndexNames indexNames
            }
        mapSchemaNames addNames entity.Root.Schema

        { ColumnFields = entity.ColumnFields
          ComputedFields = computedFields
          UniqueConstraints = entity.UniqueConstraints
          CheckConstraints = checkConstraints
          Indexes = indexes
          MainField = entity.MainField
          InsertedInternally = entity.InsertedInternally
          UpdatedInternally = entity.UpdatedInternally
          DeletedInternally = false
          TriggersMigration = entity.Source.TriggersMigration
          SaveRestoreKey = entity.Source.SaveRestoreKey
          IsHidden = entity.Source.IsHidden
          Parent = entity.Source.Parent
          Children = entity.Children
          Root = entity.Root
          TypeName = entity.TypeName
          IsAbstract = entity.Source.IsAbstract
          IsFrozen = entity.Source.IsFrozen
          HashName = entity.HashName
          RequiredFields = requiredFields
          ReferencingFields = Map.findWithDefault entityRef Map.empty referencingFields
          CascadeDeleted = Set.contains entityRef cascadeDeletedEntities
        }

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : ResolvedSchema =
        let mapEntity name entity =
            let ref = { Schema = schemaName; Name = name }
            let halfEntity = Map.find ref entities
            try
                resolveEntity ref halfEntity
            with
            | e -> raisefWithInner ResolveLayoutException e "In entity %O" name

        let entities = schema.Entities |> Map.map mapEntity

        try
            entities |> Map.values |> Seq.map (fun ent -> ent.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Entity names hash clash: %s" msg

        { Entities = entities
          ForeignConstraintNames = Map.empty
          UniqueConstraintNames = Map.empty
          CheckConstraintNames = Map.empty
          IndexNames = Map.empty
        }

    let resolveLayout () : Layout =
        let mapSchema name schema =
            try
                resolveSchema name schema
            with
            | e -> raisefWithInner ResolveLayoutException e "In schema %O" name

        let schemas = Map.map mapSchema layout.Schemas

        let addRootEntities name (schema : ResolvedSchema) =
            let names = Map.findWithDefault name emptySchemaObjectNames objectNames
            { schema with
                ForeignConstraintNames = names.ForeignConstraintNames
                UniqueConstraintNames = names.UniqueConstraintNames
                CheckConstraintNames = names.CheckConstraintNames
                IndexNames = names.IndexNames
            }

        let schemas = Map.map addRootEntities schemas
        let saveRestoredEntities = sortSaveRestoredEntities (Map.toSeq cachedSaveRestoredEntities) |> Seq.toArray

        { Schemas = schemas
          SaveRestoredEntities = saveRestoredEntities
        }

    member this.ResolveLayout () = resolveLayout ()

//
// PHASE 3: Building cases for virtual computed fields.
//

let private fillInternallyDeletedEntities (layout : Layout) (referencesMap : ReferencingEntitiesMap) (initialInternallyDeletedEntities : Set<ResolvedEntityRef>) : Set<ResolvedEntityRef> =
    let mutable internallyDeletedEntities = initialInternallyDeletedEntities

    let rec goChildren (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) =
        if not <| Set.contains entityRef internallyDeletedEntities then
            internallyDeletedEntities <- Set.add entityRef internallyDeletedEntities
            go entityRef
            for KeyValue(childRef, child) in entity.Children do
                if child.Direct then
                    let childEntity = layout.FindEntity childRef |> Option.get
                    goChildren childRef childEntity

    and go (entityRef : ResolvedEntityRef) =
        let references = Map.findWithDefault entityRef Map.empty referencesMap
        for KeyValue(refFieldRef, deleteAction) in references do
            let entity = layout.FindEntity refFieldRef.Entity |> Option.get
            // If:
            // 1. Delete action is CASCADE, or;
            // 2. Delete action is SET NULL or SET DEFAULT, and there are no check constraints using this field;
            // we can avoid marking this field as internally deleted, as it will be handled automatically by usual mechanisms.
            let addReference =
                match deleteAction with
                | RDANoAction -> true
                | RDACascade -> false
                | RDASetNull
                | RDASetDefault ->
                    entity.CheckConstraints
                    |> Map.values
                    |> Seq.exists (fun check -> tryFindUsedFieldRef refFieldRef check.UsedDatabase |> Option.isSome)
            if addReference then
                goChildren refFieldRef.Entity entity

    for ref in initialInternallyDeletedEntities do
        go ref

    internallyDeletedEntities

type private Phase3Resolver (layout : Layout, internallyDeletedEntities : Set<ResolvedEntityRef>) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, ResolvedComputedField> = Map.empty

    let resolveOneComputedField (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        let isRoot =
            match comp.Virtual with
            | None -> true
            | Some virtInfo -> Option.isNone virtInfo.InheritedFrom

        if not isRoot then
            comp
        else
            let mutable flags = emptyResolvedExprFlags
            let mutable usedDatabase = emptyUsedDatabase

            let getCaseType (case : VirtualFieldCase, currComp : ResolvedComputedField) =
                if currComp.IsMaterialized <> comp.IsMaterialized then
                  raisef ResolveLayoutException "Virtual computed fields cannot be partially materialized: %O" fieldRef
                flags <- unionResolvedExprFlags flags currComp.Flags
                usedDatabase <- unionUsedDatabases currComp.UsedDatabase usedDatabase
                Some comp.Type
            let caseTypes = computedFieldCases layout ObjectMap.empty fieldRef comp |> Seq.map getCaseType

            match unionTypes caseTypes with
            | None -> raisef ResolveLayoutException "Could not unify types for virtual field cases: %O" fieldRef
            | Some typ ->
                let root =
                    { Flags = flags
                      UsedDatabase = usedDatabase
                      Type = typ
                    }
                { comp with Root = Some root }

    let rec resolveComputedField (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        match comp.InheritedFrom with
        | Some parentRef ->
            let origFieldRef = { fieldRef with Entity = parentRef }
            let origEntity = layout.FindEntity parentRef |> Option.get
            let ret = resolveComputedField origFieldRef (Map.find fieldRef.Name origEntity.ComputedFields |> Result.get)
            { ret with InheritedFrom = Some parentRef }
        | None ->
            match Map.tryFind fieldRef cachedComputedFields with
            | Some f -> f
            | None ->
                let field = resolveOneComputedField fieldRef comp
                cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                field

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : ResolvedEntity =
        let mapComputedField name = function
            | Ok comp -> Ok <| resolveComputedField { Entity = entityRef; Name = name } comp
            | Error err -> Error err
        { entity with ComputedFields = Map.map mapComputedField entity.ComputedFields }

    let resolveSchema (schemaName : SchemaName) (schema : ResolvedSchema) : ResolvedSchema =
        let mapEntity name entity = resolveEntity { Schema = schemaName; Name = name } entity
        { schema with
            Entities = Map.map mapEntity schema.Entities
        }

    let resolveLayout () : Layout =
        let schemas = Map.map resolveSchema layout.Schemas
        { Schemas = schemas
          SaveRestoredEntities = layout.SaveRestoredEntities
        } : Layout

    member this.ResolveLayout () = resolveLayout ()

let resolveLayout (forceAllowBroken : bool) (layout : SourceLayout) : Layout =
    let phase1 = Phase1Resolver layout
    let entities = phase1.ResolveLayout ()
    let referencingFields = fillReferencingFields phase1.RootEntities entities phase1.ReferencingFields
    let cascadeDeletedEntities = fillCascadeDeletedEntities entities phase1.CascadeDeletedEntities
    let phase2 = Phase2Resolver (layout, entities, phase1.ObjectNames, referencingFields, cascadeDeletedEntities, forceAllowBroken)
    let layout2 = phase2.ResolveLayout ()
    let internallyDeletedEntities = fillInternallyDeletedEntities layout2 referencingFields phase1.InternallyDeletedEntities
    let phase3 = Phase3Resolver (layout2, internallyDeletedEntities)
    let layout3 = phase3.ResolveLayout ()
    layout3
