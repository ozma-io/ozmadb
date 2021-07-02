module FunWithFlags.FunDB.Layout.Resolve

open Newtonsoft.Json.Linq
open System.Security.Cryptography
open System.Text

open FunWithFlags.FunUtils
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
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type ResolveLayoutException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = ResolveLayoutException (message, null)

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
        | (FString s, FETScalar SFTDate) -> Option.map FDate <| SQL.trySqlDate s
        | (FString s, FETScalar SFTDateTime) -> Option.map FDateTime <| SQL.trySqlDateTime s
        | (FString s, FETScalar SFTInterval) -> Option.map FInterval <| SQL.trySqlInterval s
        | (FStringArray vals, FETArray SFTDate) -> Option.map (FDateArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlDate vals)
        | (FStringArray vals, FETArray SFTDateTime) -> Option.map (FDateTimeArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlDateTime vals)
        | (FStringArray vals, FETArray SFTInterval) -> Option.map (FIntervalArray << Array.ofSeq) (Seq.traverseOption SQL.trySqlInterval vals)
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

let private resolveEntityRef (name : EntityRef) =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ResolveLayoutException "Unspecified schema for entity %O" name

[<NoEquality; NoComparison>]
type private HalfResolvedComputedField =
    { Source : SourceComputedField
      ColumnName : SQL.SQLName
      HashName : HashName
      InheritedFrom : ResolvedEntityRef option
      VirtualInheritedFrom : ResolvedEntityRef option
    } with
        interface IComputedFieldBits with
            member this.AllowBroken = this.Source.AllowBroken
            member this.InheritedFrom = this.InheritedFrom
            member this.IsVirtual = this.Source.IsVirtual

[<NoEquality; NoComparison>]
type private HalfResolvedEntity =
    { ColumnFields : Map<FieldName, ResolvedColumnField>
      ComputedFields : Map<FieldName, HalfResolvedComputedField>
      Children : Map<ResolvedEntityRef, ChildEntity>
      TypeName : string
      Root : ResolvedEntityRef
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
            member this.MainField = this.Source.MainField
            member this.TypeName = this.TypeName
            member this.IsAbstract = this.Source.IsAbstract
            member this.IsHidden = this.Source.IsHidden
            member this.Parent = this.Source.Parent
            member this.Children = this.Children


type private HalfResolvedEntitiesMap = Map<ResolvedEntityRef, HalfResolvedEntity>

let private resolveUniqueConstraint (entity : HalfResolvedEntity) (constrName : ConstraintName) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.Columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    for name in constr.Columns do
        if not <| Map.containsKey name entity.ColumnFields then
            raisef ResolveLayoutException "Unknown column %O in unique constraint" name

    { Columns = constr.Columns
      HashName = makeHashName constrName
    }

let private sqlColumnName (ref : ResolvedEntityRef) (entity : SourceEntity) (fieldName : FieldName) : SQL.SQLName =
    let str =
        match entity.Parent with
        | None -> string fieldName
        | Some p when p.Schema = ref.Schema -> sprintf "%O__%O" ref.Name fieldName
        | Some p -> sprintf "%O__%O__%O" ref.Schema ref.Name fieldName
    SQL.SQLName (makeHashNameFor SQL.sqlIdentifierLength str)

//
// PHASE 1: Building column fields.
//

type private Phase1Resolver (layout : SourceLayout) =
    let mutable cachedEntities : HalfResolvedEntitiesMap = Map.empty
    let mutable rootEntities : Set<ResolvedEntityRef> = Set.empty

    let unionComputedField (name : FieldName) (parent : HalfResolvedComputedField) (child : HalfResolvedComputedField) =
        if parent.Source.IsVirtual && child.Source.IsVirtual then
            let virtualInherited =
                match parent.VirtualInheritedFrom with
                | None ->
                    // `parent` is root virtual field. Parents always have `InheritedFrom`, so we can use it in this case.
                    Option.get parent.InheritedFrom
                | Some inherited -> inherited

            { child with VirtualInheritedFrom = Some virtualInherited }
        else
            raisef ResolveLayoutException "Computed field names clash: %O" name

    let resolveFieldType (ref : ResolvedEntityRef) (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference entityRef ->
            let resolvedRef = resolveEntityRef entityRef
            let refEntity =
                match layout.FindEntity(resolvedRef) with
                | Some refEntity when not refEntity.IsHidden -> refEntity
                | _ -> raisef ResolveLayoutException "Cannot find entity %O from reference type" resolvedRef
            if refEntity.ForbidExternalReferences && ref.Schema <> resolvedRef.Schema then
                raisef ResolveLayoutException "References from other schemas to entity %O are forbidden" entityRef
            FTReference resolvedRef
        | FTEnum vals ->
            if Set.isEmpty vals then
                raisef ResolveLayoutException "Enums must not be empty"
            FTEnum vals

    let resolveColumnField (ref : ResolvedEntityRef) (entity : SourceEntity) (fieldName : FieldName) (col : SourceColumnField) : ResolvedColumnField =
        let fieldType =
            match parse tokenizeFunQL fieldType col.Type with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing column field type %s: %s" col.Type msg
        let defaultValue =
            match col.DefaultValue with
            | None -> None
            | Some def ->
                match parse tokenizeFunQL fieldExpr def with
                | Ok r ->
                    match reduceDefaultExpr r with
                    | Some FNull -> raisef ResolveLayoutException "Default expression cannot be NULL"
                    | Some v -> Some v
                    | None -> raisef ResolveLayoutException "Default expression is not trivial: %s" def
                | Error msg -> raisef ResolveLayoutException "Error parsing column field default expression: %s" msg
        let fieldType = resolveFieldType ref entity fieldType

        { FieldType = fieldType
          ValueType = compileFieldType fieldType
          DefaultValue = defaultValue
          IsNullable = col.IsNullable
          IsImmutable = col.IsImmutable
          InheritedFrom = None
          ColumnName = sqlColumnName ref entity fieldName
          HashName = makeHashName fieldName
        }

    let resolveEntity (parent : HalfResolvedEntity option) (entityRef : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        let mapColumnField name field =
            try
                checkFieldName name
                resolveColumnField entityRef entity name field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In column field %O" name
        let mapComputedField name (field : SourceComputedField) =
            try
                checkFieldName name
                { Source = field
                  InheritedFrom = None
                  VirtualInheritedFrom = None
                  ColumnName = sqlColumnName entityRef entity name
                  HashName = makeHashName name
                }
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In computed field %O" name

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
                    | Some pref -> Some pref
                let inheritedFields = Map.map (fun name field -> { field with InheritedFrom = addParent field.InheritedFrom } : HalfResolvedComputedField) p.ComputedFields
                Map.unionWith unionComputedField inheritedFields selfComputedFields

        try
            let columnNames = selfColumnFields |> Map.values |> Seq.map (fun field -> field.HashName)
            let computedNames = selfComputedFields |> Map.values |> Seq.map (fun field -> field.HashName)
            Seq.append columnNames computedNames |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Field names hash clash: %s" msg

        let root =
            match parent with
            | None ->
                rootEntities <- Set.add entityRef rootEntities
                entityRef
            | Some p -> p.Root

        let typeName =
            if root.Schema = entityRef.Schema then
                entityRef.Name.ToString()
            else
                sprintf "%O__%O" entityRef.Schema entityRef.Name

        let ret =
            { ColumnFields = columnFields
              ComputedFields = computedFields
              Children = Map.empty
              Root = root
              TypeName = typeName
              Source = entity
            }

        if not (entity.MainField = funId
                    || (entity.MainField = funSubEntity && hasSubType ret)
                    || Map.containsKey entity.MainField columnFields
                    || Map.containsKey entity.MainField computedFields) then
                raisef ResolveLayoutException "Nonexistent main field: %O" entity.MainField

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
        | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In entity %O" ref

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
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In schema %O" name
        Map.iter iterSchema layout.Schemas

    let checkEntityColumnNames (rootEntity : HalfResolvedEntity) =
        let collectColumns (entity : HalfResolvedEntity) =
            let cols = entity.ColumnFields |> Map.values |> Seq.mapMaybe (fun field -> if Option.isNone field.InheritedFrom then Some field.ColumnName else None)
            let comps = entity.ComputedFields |> Map.values |> Seq.mapMaybe (fun field -> if Option.isNone field.InheritedFrom then Some field.ColumnName else None)
            Seq.append cols comps
        
        let selfColumns = collectColumns rootEntity
        let childrenColumns = rootEntity.Children |> Map.keys |> Seq.collect (fun ref -> collectColumns (Map.find ref cachedEntities))
        try
            Seq.append selfColumns childrenColumns |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Column names clash: %s" msg

    member this.ResolveLayout () =
        resolveLayout ()
        for root in rootEntities do
            let entity = Map.find root cachedEntities
            checkEntityColumnNames entity
        cachedEntities

let private parseRelatedExpr (rawExpr : string) =
    match parse tokenizeFunQL fieldExpr rawExpr with
    | Ok r -> r
    | Error msg -> raisef ResolveLayoutException "Error parsing local expression: %s" msg

let private relatedResolutionFlags = { emptyExprResolutionFlags with Privileged = true }

//
// PHASE 2: Building computed fields, sub entity parse expressions and other stuff.
//

type private RelatedExpr =
    { Info : ExprInfo
      References : UsedReferences
      Expr : ResolvedFieldExpr
    }

type private Phase2Resolver (layout : SourceLayout, entities : HalfResolvedEntitiesMap, forceAllowBroken : bool) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, Result<ResolvedComputedField, exn>> = Map.empty

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
                    | Error e -> raisefWithInner ResolveLayoutException e "Computed field %O is broken" fieldRef
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
                member this.MainField = currEnt.Source.MainField
                member this.TypeName = currEnt.TypeName
                member this.IsAbstract = currEnt.Source.IsAbstract
                member this.IsHidden = currEnt.Source.IsHidden
                member this.Parent = currEnt.Source.Parent
                member this.Children = currEnt.Children
            }

        let mutable cachedEntities = Map.empty
        { new ILayoutBits with
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

    and resolveRelatedExpr (wrappedLayout : ILayoutBits) (entityRef : ResolvedEntityRef) (expr : ParsedFieldExpr) : RelatedExpr =
        let entityInfo = SFEntity entityRef
        let (localArguments, expr) =
            try
                resolveSingleFieldExpr wrappedLayout Map.empty localExprFromEntityId relatedResolutionFlags entityInfo expr
            with
            | :? ViewResolveException as e -> raisefWithInner ResolveLayoutException e ""
        let (exprInfo, usedReferences) = fieldExprUsedReferences wrappedLayout expr
        if exprInfo.HasAggregates then
            raisef ResolveLayoutException "Aggregate functions are not allowed here"
        { Info = exprInfo
          References = usedReferences
          Expr = expr
        }

    and resolveOneComputeField (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (comp : HalfResolvedComputedField) : ResolvedComputedField =
        let wrappedLayout = makeWrappedLayout stack
        // Computed fields are always assumed to reference immediate fields. Also read shortcomings of current computed field compilation in Compile.fs.
        let expr = resolveRelatedExpr wrappedLayout entityRef (parseRelatedExpr comp.Source.Expression)
        if comp.Source.IsMaterialized then
            if expr.Info.HasQuery then
                raisef ResolveLayoutException "Queries are not supported in materialized computed fields"
            if not <| Set.isEmpty expr.References.UsedArguments then
                raisef ResolveLayoutException "Arguments are not allowed in materialized computed fields"
        let exprType =
            try
                typecheckFieldExpr wrappedLayout expr.Expr
            with
            | :? ViewTypecheckException as e -> raisefWithInner ResolveLayoutException e "Failed to typecheck computed column"
        let virtualInfo =
            if comp.Source.IsVirtual then
                Some
                    { // Place random stuff there for now, we calculate virtual cases in a later phase.
                      Cases = [||]
                      InheritedFrom = comp.VirtualInheritedFrom
                    }
            else
                None
        { Expression = expr.Expr
          Type = exprType
          InheritedFrom = None
          IsLocal = expr.Info.IsLocal
          AllowBroken = comp.Source.AllowBroken
          HashName = comp.HashName
          ColumnName = comp.ColumnName
          Virtual = virtualInfo
          Root = None
          IsMaterialized = comp.Source.IsMaterialized
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : HalfResolvedComputedField) : Result<ResolvedComputedField, exn> =
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
                    let field = Ok <| resolveOneComputeField newStack fieldRef.Entity entity comp
                    cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                    field
                with
                | :? ResolveLayoutException as e when comp.Source.AllowBroken || forceAllowBroken ->
                    Error (e :> exn)

    and resolveLocalExpr (entityRef : ResolvedEntityRef) (expr : ParsedFieldExpr) : RelatedExpr =
        // Local expressions are used as-is in the database, so `Immediate = false`.
        let expr = resolveRelatedExpr initialWrappedLayout entityRef expr
        if not expr.Info.IsLocal then
            raisef ResolveLayoutException "Non-local expressions are not allowed here"
        if not <| Set.isEmpty expr.References.UsedArguments then
            raisef ResolveLayoutException "Arguments are not allowed here"
        expr

    let resolveCheckConstraint (entityRef : ResolvedEntityRef) (constrName : ConstraintName) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let expr = resolveRelatedExpr initialWrappedLayout entityRef (parseRelatedExpr constr.Expression)
        if expr.Info.HasQuery then
            raisef ResolveLayoutException "Subqueries are not allowed here"
        if not <| Set.isEmpty expr.References.UsedArguments then
            raisef ResolveLayoutException "Arguments are not allowed here"
        { Expression = expr.Expr
          UsedSchemas = expr.References.UsedSchemas
          IsLocal = expr.Info.IsLocal
          HashName = makeHashName constrName
        }

    let resolveIndexColumn (entityRef : ResolvedEntityRef) (indexType : IndexType) (colExpr : string) : ResolvedIndexColumn =
        let col =
            match parse tokenizeFunQL indexColumn colExpr with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing index expression: %s" msg

        let expr = resolveLocalExpr entityRef col.Expr
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
        { Expr = expr.Expr
          OpClass = col.OpClass
          Order = order
          Nulls = nullsOrder
        }

    let resolveIndex (entityRef : ResolvedEntityRef) (indexName : IndexName) (index : SourceIndex) : ResolvedIndex =
        if Array.isEmpty index.Expressions then
            raise <| ResolveLayoutException "Empty index"

        let entity = initialWrappedLayout.FindEntity entityRef |> Option.get
        let resolveIncludedExpr x =
            let expr = resolveLocalExpr entityRef (parseRelatedExpr x)
            let isGood =
                match expr with
                | { Expr = FERef { Ref = { Ref = VRColumn col } } } ->
                    match entity.FindField col.Name with
                    | Some { Field = RColumnField _ }
                    | Some { Field = RId }
                    | Some { Field = RSubEntity } -> true
                    | _ -> false
                | _ -> false
            if not isGood then
                raisef ResolveLayoutException "Expressions are not supported as included index columns"
            expr
        
        let exprs = Array.map (resolveIndexColumn entityRef index.Type) index.Expressions
        let includedExprs = Array.map (fun x -> (resolveLocalExpr entityRef (parseRelatedExpr x)).Expr) index.IncludedExpressions
        let predicate = Option.map (fun x -> (resolveLocalExpr entityRef (parseRelatedExpr x)).Expr) index.Predicate

        { Expressions = exprs
          IncludedExpressions = includedExprs
          HashName = makeHashName indexName
          IsUnique = index.IsUnique
          Predicate = predicate
          Type = index.Type
        }

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ErroredEntity * ResolvedEntity =
        let mutable computedErrors = Map.empty

        let mapComputedField name field =
            try
                let ret = resolveComputedField Set.empty entity { Entity = entityRef; Name = name } field
                match ret with
                | Error e ->
                    if Option.isNone field.InheritedFrom && field.Source.AllowBroken then
                        computedErrors <- Map.add name e computedErrors
                    Error e
                | Ok r -> Ok r
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In computed field %O" name

        let computedFields = Map.map mapComputedField entity.ComputedFields

        // Ideally we would use `tempEntity` for it too, but the code for `resolveRelatedExpr` is already here, so...
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint entityRef name constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In check constraint %O" name
        let checkConstraints = Map.map mapCheckConstraint entity.Source.CheckConstraints

        try
            checkConstraints |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Check constraint names clash (first %i characters): %s" hashNameLength msg

        let mapUniqueConstraint name constr =
            try
                checkName name
                resolveUniqueConstraint entity name constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In unique constraint %O" name
        let uniqueConstraints = Map.map mapUniqueConstraint entity.Source.UniqueConstraints

        try
            uniqueConstraints |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Unique constraint names clash (first %i characters): %s" hashNameLength msg

        let mapIndex name index =
            try
                checkName name
                resolveIndex entityRef name index
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In index %O" name
        let indexes = Map.map mapIndex entity.Source.Indexes

        try
            indexes |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Index names clash (first %i characters): %s" hashNameLength msg

        let wrappedEntity = initialWrappedLayout.FindEntity entityRef |> Option.get
        // Check that main field is valid.
        wrappedEntity.FindField funMain |> Option.get |> ignore

        let ret =
            { ColumnFields = entity.ColumnFields
              ComputedFields = computedFields
              UniqueConstraints = uniqueConstraints
              CheckConstraints = checkConstraints
              Indexes = indexes
              MainField = entity.Source.MainField
              ForbidExternalReferences = entity.Source.ForbidExternalReferences
              ForbidTriggers = entity.Source.ForbidTriggers
              TriggersMigration = entity.Source.TriggersMigration
              IsHidden = entity.Source.IsHidden
              Parent = entity.Source.Parent
              Children = entity.Children
              Root = entity.Root
              TypeName = entity.TypeName
              IsAbstract = entity.Source.IsAbstract
              IsFrozen = entity.Source.IsFrozen
              HashName = makeHashName entityRef.Name
            } : ResolvedEntity
        let errors =
            { ComputedFields = computedErrors
            }
        (errors, ret)

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : ErroredSchema * ResolvedSchema =
        let mutable errors = Map.empty

        let mapEntity name entity =
            let ref = { Schema = schemaName; Name = name }
            let halfEntity = Map.find ref entities
            try
                let (entityErrors, entity) = resolveEntity ref halfEntity
                if not (Map.isEmpty entityErrors.ComputedFields) then
                    errors <- Map.add name entityErrors errors
                entity
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In entity %O" name

        let entities = schema.Entities |> Map.map mapEntity

        try
            entities |> Map.values |> Seq.map (fun ent -> ent.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Entity names hash clash: %s" msg

        let ret =
            { Entities = entities
            } : ResolvedSchema
        (errors, ret)

    let resolveLayout () : ErroredLayout * Layout =
        let mutable errors = Map.empty

        let mapSchema name schema =
            try
                let (schemaErrors, ret) = resolveSchema name schema
                if not <| Map.isEmpty schemaErrors then
                    errors <- Map.add name schemaErrors errors
                ret
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e "In schema %O" name

        let ret =
            { Schemas = Map.map mapSchema layout.Schemas
            } : Layout
        (errors, ret)

    member this.ResolveLayout () = resolveLayout ()

//
// PHASE 3: Building cases for virtual computed fields.
//

type private Phase3Resolver (layout : Layout) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, ResolvedComputedField> = Map.empty
    let mutable cachedCaseExpressions : Map<ResolvedFieldRef, VirtualFieldCase array> = Map.empty

    let rec resolveMyVirtualCases (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : VirtualFieldCase array =
        let childVirtualCases = findVirtualChildren entity fieldRef.Name |> Seq.toArray
        let handledChildren = childVirtualCases |> Seq.map (fun c -> c.PossibleEntities) |> Set.unionMany

        let isMyselfHandled (ref, entity : ResolvedEntity) =
            not (entity.IsAbstract || Set.contains ref handledChildren)

        let childrenEntities = entity.Children |> Map.keys |> Seq.map (fun ref -> (ref, layout.FindEntity ref |> Option.get))
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

    and resolveVirtualCases (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : VirtualFieldCase[] =
        match Map.tryFind fieldRef cachedCaseExpressions with
        | Some cached -> cached
        | None ->
            let cases = resolveMyVirtualCases entity fieldRef comp
            cachedCaseExpressions <- Map.add fieldRef cases cachedCaseExpressions
            cases

    // We perform a DFS here, ordering virtual fields children-first.
    and findVirtualChildren (entity : ResolvedEntity) (fieldName : FieldName) : VirtualFieldCase seq =
        entity.Children |> Map.toSeq |> Seq.collect (fun (ref, child) -> findVirtualChild fieldName ref child)

    and findVirtualChild (fieldName : FieldName) (entityRef : ResolvedEntityRef) (child : ChildEntity) : VirtualFieldCase seq =
        if not child.Direct then
            Seq.empty
        else
            let childEntity = layout.FindEntity entityRef |> Option.get
            let childField = Map.find fieldName childEntity.ComputedFields
            match childField with
            | Ok f when Option.isNone f.InheritedFrom ->
                resolveVirtualCases childEntity { Entity = entityRef; Name = fieldName } f |> Array.toSeq
            | _ -> findVirtualChildren childEntity fieldName

    let resolveOneComputedField (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        let (comp, isRoot) =
            match comp.Virtual with
            | None -> (comp, true)
            | Some virtInfo ->
                let cases = resolveVirtualCases entity fieldRef comp
                let virtInfo = { virtInfo with Cases = cases }
                ({ comp with Virtual = Some virtInfo }, Option.isNone virtInfo.InheritedFrom)

        if not isRoot then
            comp
        else
            let mutable isLocal = true
            let getCaseType (case, currComp : ResolvedComputedField) =
                if currComp.IsMaterialized <> comp.IsMaterialized then
                  raisef ResolveLayoutException "Virtual computed fields cannot be partially materialized: %O" fieldRef
                if not comp.IsLocal then
                    isLocal <- false
                comp.Type
            let caseTypes = computedFieldCases layout ObjectMap.empty fieldRef comp |> Seq.map getCaseType
            match unionTypes caseTypes with
            | None -> raisef ResolveLayoutException "Could not unify types for virtual field cases: %O" fieldRef
            | Some typ ->
                let root =
                    { IsLocal = isLocal
                      Type = typ
                    }
                { comp with Root = Some root }

    let rec resolveComputedField (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        match comp.InheritedFrom with
        | Some parentRef ->
            let origFieldRef = { fieldRef with Entity = parentRef }
            let origEntity = layout.FindEntity parentRef |> Option.get
            let ret = resolveComputedField origEntity origFieldRef (Map.find fieldRef.Name origEntity.ComputedFields |> Result.get)
            { ret with InheritedFrom = Some parentRef }
        | None ->
            match Map.tryFind fieldRef cachedComputedFields with
            | Some f -> f
            | None ->
                let field = resolveOneComputedField entity fieldRef comp
                cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                field

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : ResolvedEntity =
        let mapComputedField name = function
            | Ok comp -> Ok <| resolveComputedField entity { Entity = entityRef; Name = name } comp
            | Error err -> Error err
        { entity with ComputedFields = Map.map mapComputedField entity.ComputedFields }

    let resolveSchema (schemaName : SchemaName) (schema : ResolvedSchema) : ResolvedSchema =
        let mapEntity name entity = resolveEntity { Schema = schemaName; Name = name } entity
        { Entities = Map.map mapEntity schema.Entities
        }

    let resolveLayout () : Layout =
        let schemas = Map.map resolveSchema layout.Schemas
        { Schemas = schemas
        } : Layout

    member this.ResolveLayout () = resolveLayout ()

let resolveLayout (layout : SourceLayout) (forceAllowBroken : bool) : ErroredLayout * Layout =
    let phase1 = Phase1Resolver layout
    let entities = phase1.ResolveLayout ()
    let phase2 = Phase2Resolver (layout, entities, forceAllowBroken)
    let (errors, layout2) = phase2.ResolveLayout ()
    let phase3 = Phase3Resolver layout2
    let layout3 = phase3.ResolveLayout ()
    (errors, layout3)
