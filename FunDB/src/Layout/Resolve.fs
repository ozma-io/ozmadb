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
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
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

let private makeHashName (FunQLName name) =
    if String.length name <= hashNameLength then
        name
    else
        assert (hashNameLength >= hashNameSuffixLength)
        let prefix = String.truncate (hashNameLength - hashNameSuffixLength) name
        use md5 = MD5.Create ()
        let md5Bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(name))
        let stringBuilder = StringBuilder ()
        for i = 1 to hashNameSuffixLength / 2 do
            ignore <| stringBuilder.Append(md5Bytes.[0].ToString("x2"))
        sprintf "%s%s" prefix (stringBuilder.ToString())

let private resolveEntityRef (name : EntityRef) =
    match tryResolveEntityRef name with
    | Some ref -> ref
    | None -> raisef ResolveLayoutException "Unspecified schema for entity %O" name

[<NoEquality; NoComparison>]
type private HalfResolvedComputedField =
    | HRInherited of ResolvedEntityRef
    | HRSource of HashName * SourceComputedField

[<NoEquality; NoComparison>]
type private HalfResolvedEntity =
    { ColumnFields : Map<FieldName, ResolvedColumnField>
      ComputedFields : Map<FieldName, HalfResolvedComputedField>
      Children : Map<ResolvedEntityRef, ChildEntity>
      TypeName : string
      Root : ResolvedEntityRef
      Source : SourceEntity
    } with
        member this.FindField name =
            genericFindField (fun name -> Map.tryFind name this.ColumnFields) (fun name -> Map.tryFind name this.ComputedFields) this name

        interface IEntityFields with
            member this.FindField name =
                genericFindField (fun name -> Map.tryFind name this.ColumnFields) (fun _ -> None) this name
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
            member this.IsAbstract = this.Source.IsAbstract
            member this.Parent = this.Source.Parent
            member this.Children = Map.toSeq this.Children

type private HalfResolvedEntities = Map<ResolvedEntityRef, HalfResolvedEntity>

let private subEntityColumn = SQL.VEColumn { table = None; name = sqlFunSubEntity }

let private makeCheckExprGeneric (getTypeName : 'a -> string) (getIsAbstract : 'a -> bool) (getChildren : 'a -> Map<ResolvedEntityRef, ChildEntity>) (getEntity : ResolvedEntityRef -> 'a) (entity : 'a) =
    let getName entity =
        let isAbstract = getIsAbstract entity
        if isAbstract then
            None
        else
            entity |> getTypeName |> SQL.VString |> SQL.VEValue |> Some

    let childrenEntities = getChildren entity |> Map.keys |> Seq.map getEntity
    let allEntities = Seq.append (Seq.singleton entity) childrenEntities
    let options = allEntities |> Seq.mapMaybe getName |> Seq.toArray

    if Array.isEmpty options then
        SQL.VEValue (SQL.VBool false)
    else if Array.length options = 1 then
        SQL.VEBinaryOp (subEntityColumn, SQL.BOEq, options.[0])
    else
        SQL.VEIn (subEntityColumn, options)

let private makeCheckHalfExpr = makeCheckExprGeneric (fun (x : HalfResolvedEntity) -> x.TypeName) (fun x -> x.Source.IsAbstract) (fun x -> x.Children)

let makeCheckExpr = makeCheckExprGeneric (fun (x : ResolvedEntity) -> x.TypeName) (fun x -> x.IsAbstract) (fun x -> x.Children)

let private resolveUniqueConstraint (entity : ResolvedEntity) (constrName : ConstraintName) (constr : SourceUniqueConstraint) : ResolvedUniqueConstraint =
    if Array.isEmpty constr.Columns then
        raise <| ResolveLayoutException "Empty unique constraint"

    let checkColumn name =
        match entity.FindField(name) with
        | Some _ -> name
        | None -> raisef ResolveLayoutException "Unknown column %O in unique constraint" name

    { Columns = Array.map checkColumn constr.Columns
      HashName = makeHashName constrName
    }

//
// PHASE 1: Building column fields.
//

type private Phase1Resolver (layout : SourceLayout) =
    let mutable cachedEntities : HalfResolvedEntities = Map.empty

    let unionComputedField (name : FieldName) (parent : HalfResolvedComputedField) (child : HalfResolvedComputedField) =
        let (nameHash, childSource) =
            match child with
            | HRInherited _ -> failwith "Impossible"
            | HRSource (nameHash, childSource) -> (nameHash, childSource)
        let parentRef =
            match parent with
            | HRSource _ -> failwith "Impossible"
            | HRInherited parentRef -> parentRef
        let parentEntity = layout.FindEntity parentRef |> Option.get
        let parentSource = Map.find name parentEntity.ComputedFields

        if parentSource.IsVirtual && childSource.IsVirtual then
            child
        else
            raisef ResolveLayoutException "Computed field names clash: %O" name

    let resolveFieldType (ref : ResolvedEntityRef) (entity : SourceEntity) : ParsedFieldType -> ResolvedFieldType = function
        | FTType ft -> FTType ft
        | FTReference entityRef ->
            let resolvedRef = resolveEntityRef entityRef
            let refEntity =
                match layout.FindEntity(resolvedRef) with
                | None -> raisef ResolveLayoutException "Cannot find entity %O from reference type" resolvedRef
                | Some refEntity -> refEntity
            if refEntity.ForbidExternalReferences && ref.schema <> resolvedRef.schema then
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

        let columnName =
            match entity.Parent with
            | None -> fieldName.ToString()
            | Some p when p.schema = ref.schema -> sprintf "%O__%O" ref.name fieldName
            | Some p -> sprintf "%O__%O__%O" ref.schema ref.name fieldName

        { FieldType = fieldType
          ValueType = compileFieldType fieldType
          DefaultValue = defaultValue
          IsNullable = col.IsNullable
          IsImmutable = col.IsImmutable
          InheritedFrom = None
          ColumnName = SQL.SQLName columnName
          HashName = makeHashName fieldName
        }

    let resolveEntity (parent : HalfResolvedEntity option) (entityRef : ResolvedEntityRef) (entity : SourceEntity) : HalfResolvedEntity =
        let mapColumnField name field =
            try
                checkFieldName name
                resolveColumnField entityRef entity name field
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In column field %O: %s" name e.Message
        let mapComputedField name (field : SourceComputedField) =
            try
                checkFieldName name
                HRSource (makeHashName name, field)
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In computed field %O: %s" name e.Message

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
                    | HRSource (hashName, source) -> HRInherited parentRef

                    | HRInherited pref -> HRInherited pref
                let inheritedFields = Map.map (fun name field -> addParent field) p.ComputedFields
                Map.unionWith unionComputedField inheritedFields selfComputedFields

        try
            let columnNames = selfColumnFields |> Map.values |> Seq.map (fun field -> field.HashName)
            let computedNames = selfComputedFields |> Map.values |> Seq.map (function | HRSource (hashName, comp) -> hashName | _ -> failwith "impossible")
            Seq.append columnNames computedNames |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Field names hash clash: %s" msg

        let root =
            match parent with
            | None -> entityRef
            | Some p -> p.Root

        let typeName =
            if root.schema = entityRef.schema then
                entityRef.name.ToString()
            else
                sprintf "%O__%O" entityRef.schema entityRef.name

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
                    if parentRef.schema <> ref.schema then
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
                checkName ref.name
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
        | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In entity %O: %s" ref e.Message

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) =
        let rec iterEntity name entity =
            let ref = { schema = schemaName; name = name }
            let entity = resolveOneEntity Set.empty None ref entity
            ()
        schema.Entities |> Map.iter iterEntity

    let resolveLayout () =
        let iterSchema name schema =
            try
                checkSchemaName name
                resolveSchema name schema
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In schema %O: %s" name e.Message
        Map.iter iterSchema layout.Schemas

    member this.ResolveLayout () =
        resolveLayout ()
        cachedEntities

type private ExprProperties =
    { IsLocal : bool
      HasId : bool
      UsedSchemas : UsedSchemas
    }

let private emptyExprProperties : ExprProperties =
    { IsLocal = true
      HasId = false
      UsedSchemas = Map.empty
    }

let private unionExprProperties (a : ExprProperties) (b : ExprProperties) : ExprProperties =
    { IsLocal = a.IsLocal && b.IsLocal
      HasId = a.HasId || b.HasId
      UsedSchemas = mergeUsedSchemas a.UsedSchemas b.UsedSchemas
    }

//
// PHASE 2: Building computed fields, sub entity parse expressions and other stuff.
//

type private Phase2Resolver (layout : SourceLayout, entities : HalfResolvedEntities, forceAllowBroken : bool) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, Result<ResolvedComputedField, exn>> = Map.empty

    let layoutFields =
        { new ILayoutFields with
              member this.FindEntity ref = Map.tryFind ref entities |> Option.map (fun e -> e :> IEntityFields)
        }

    let rec checkPath (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) (fields : FieldName list) : ExprProperties =
        match fields with
        | [] ->
            match entity.FindField fieldRef.name with
            | Some { Field = RId } ->
                let usedSchemas = addUsedEntityRef fieldRef.entity Map.empty
                { IsLocal = true; HasId = true; UsedSchemas = usedSchemas }
            | Some { Field = RSubEntity } ->
                let usedSchemas = addUsedEntityRef fieldRef.entity Map.empty
                { IsLocal = true; HasId = false; UsedSchemas = usedSchemas }
            | Some { Field = RComputedField comp } ->
                match resolveComputedField stack entity fieldRef comp with
                | Ok field -> { IsLocal = field.IsLocal; HasId = field.HasId; UsedSchemas = field.UsedSchemas }
                | Error e -> raisefWithInner ResolveLayoutException e "Computed field %O is broken" fieldRef
            | Some { Name = newName; Field = RColumnField _ } ->
                let usedSchemas = addUsedField fieldRef.entity.schema fieldRef.entity.name newName Map.empty
                { IsLocal = true; HasId = false; UsedSchemas = usedSchemas }
            | None -> raisef ResolveLayoutException "Column field not found in path: %O" fieldRef.name
        | (ref :: refs) ->
            match Map.tryFind fieldRef.name entity.ColumnFields with
            | Some { FieldType = FTReference refEntity } ->
                let newEntity = Map.find refEntity entities
                let ret = checkPath stack newEntity { entity = refEntity; name = ref } refs
                { ret with
                      IsLocal = false
                      UsedSchemas = addUsedFieldRef fieldRef ret.UsedSchemas
                }
            | _ -> raisef ResolveLayoutException "Invalid dereference in path: %O" ref

    and resolveRelatedExpr (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (rawExpr : string) : ExprProperties * ResolvedFieldExpr =
        let expr =
            match parse tokenizeFunQL fieldExpr rawExpr with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing local expression: %s" msg

        let mutable props = emptyExprProperties

        let resolveReference : LinkedFieldRef -> LinkedBoundFieldRef = function
            | { Ref = VRColumn { entity = None; name = name }; Path = path } ->
                let res = checkPath stack entity { entity = entityRef; name = name } (Array.toList path)
                props <- unionExprProperties props res
                let bound =
                    { Ref = { entity = entityRef; name = name }
                      Immediate = true
                    }
                { Ref = VRColumn { Ref = ({ entity = None; name = name } : FieldRef); Bound = Some bound }; Path = path }
            // Placeholders are forbidden because computed fields might be used in check expressions.
            | ref ->
                raisef ResolveLayoutException "Invalid reference in computed column: %O" ref
        let resolveQuery query =
            props <- { props with IsLocal = false }
            try
                let (_, res) = resolveSelectExpr layoutFields query
                res
            with
            | :? ViewResolveException as e -> raisefWithInner ResolveLayoutException e.InnerException "Invalid expression"
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in computed columns"

        let exprRes =
            mapFieldExpr
                { idFieldExprMapper resolveReference resolveQuery with
                      Aggregate = voidAggr
                      SubEntity = resolveSubEntity layoutFields
                } expr

        (props, exprRes)

    and resolveOneComputedExpr (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (hashName : HashName) (comp : SourceComputedField) : ResolvedComputedField =
        let (props, exprRes) = resolveRelatedExpr stack entityRef entity comp.Expression
        { Expression = exprRes
          IsLocal = props.IsLocal
          HasId = props.HasId
          UsedSchemas = props.UsedSchemas
          InheritedFrom = None
          AllowBroken = comp.AllowBroken
          HashName = hashName
          // Place random stuff there for now, we calculate virtual cases in a later phase.
          VirtualCases = if comp.IsVirtual then Some [||] else None
        }

    and resolveComputedField (stack : Set<ResolvedFieldRef>) (entity : HalfResolvedEntity) (fieldRef : ResolvedFieldRef) : HalfResolvedComputedField -> Result<ResolvedComputedField, exn> = function
        | HRInherited parentRef ->
            let origFieldRef = { fieldRef with entity = parentRef }
            match Map.tryFind origFieldRef cachedComputedFields with
            | Some (Ok f) -> Ok { f with InheritedFrom = Some parentRef }
            | Some (Error e) -> Error e
            | None ->
                let origEntity = Map.find parentRef entities
                match resolveComputedField stack origEntity origFieldRef (Map.find fieldRef.name origEntity.ComputedFields) with
                | Ok f -> Ok { f with InheritedFrom = Some parentRef }
                | Error e -> Error e
        | HRSource (hashName, comp) ->
            match Map.tryFind fieldRef cachedComputedFields with
            | Some f -> f
            | None ->
                if Set.contains fieldRef stack then
                    raisef ResolveLayoutException "Cycle detected in computed fields: %O" stack
                let newStack = Set.add fieldRef stack
                try
                    let field = Ok <| resolveOneComputedExpr newStack fieldRef.entity entity hashName comp
                    cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                    field
                with
                | :? ResolveLayoutException as e when comp.AllowBroken || forceAllowBroken ->
                    Error (e :> exn)

    let resolveLocalExpr (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (rawExpr : string) : LocalFieldExpr =
        let expr =
            match parse tokenizeFunQL fieldExpr rawExpr with
            | Ok r -> r
            | Error msg -> raisef ResolveLayoutException "Error parsing expression: %s" msg
        let resolveReference : LinkedFieldRef -> FieldName = function
            | { Ref = VRColumn { entity = None; name = name }; Path = [||] } ->
                let res =
                    match Map.tryFind name entity.ColumnFields with
                    | Some col -> name
                    | None ->
                        match Map.tryFind name entity.ComputedFields with
                        | Some (Ok comp) ->
                            if comp.IsLocal then
                                name
                            else
                                raisef ResolveLayoutException "Non-local computed field reference in local expression: %O" name
                        | _ when name = funId -> funId
                        | _ when name = funSubEntity -> funSubEntity
                        | _ -> raisef ResolveLayoutException "Column not found in local expression: %O" name
                res
            | ref ->
                raisef ResolveLayoutException "Invalid reference in local expression: %O" ref
        let voidQuery query =
            raisef ResolveLayoutException "Queries are not allowed in local expressions: %O" query
        let voidAggr aggr =
            raisef ResolveLayoutException "Aggregate functions are not allowed in local expressions"
        let resolveLocalSubEntity ctx (field : FieldName) subEntity =
            let fieldRef = { entity = entityRef; name = field }
            let boundField = { Ref = fieldRef; Immediate = true }
            let linkedField = { Ref = VRColumn { Ref = { entity = None; name = field }; Bound = Some boundField }; Path = [||] } : LinkedBoundFieldRef
            resolveSubEntity layoutFields ctx linkedField subEntity

        mapFieldExpr
            { idFieldExprMapper resolveReference voidQuery with
                  Aggregate = voidAggr
                  SubEntity = resolveLocalSubEntity
            } expr

    let resolveCheckConstraint (stack : Set<ResolvedFieldRef>) (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) (constrName : ConstraintName) (constr : SourceCheckConstraint) : ResolvedCheckConstraint =
        let (opts, expr) = resolveRelatedExpr stack entityRef entity constr.Expression
        if opts.HasId then
            raisef ResolveLayoutException "Cannot use id in check constraints"
        { Expression = expr
          IsLocal = opts.IsLocal
          HashName = makeHashName constrName
        }

    let resolveIndex (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) (indexName : IndexName) (index : SourceIndex) : ResolvedIndex =
        if Array.isEmpty index.Expressions then
            raise <| ResolveLayoutException "Empty index"

        let parseExpr rawExpr =
            match resolveLocalExpr entityRef entity rawExpr with
            | FERef r when r = funId || r = funSubEntity -> raisef ResolveLayoutException "Cannot index system column: %s" (r.ToFunQLString())
            | ret -> ret

        { Expressions = Array.map parseExpr index.Expressions
          HashName = makeHashName indexName
          IsUnique = index.IsUnique
        }

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : HalfResolvedEntity) : ErroredEntity * ResolvedEntity =
        let mutable computedErrors = Map.empty

        let mapComputedField name field =
            try
                let ret = resolveComputedField Set.empty entity { entity = entityRef; name = name } field
                match ret with
                | Error e ->
                    match field with
                    | HRSource (_, comp) when not comp.AllowBroken ->
                        computedErrors <- Map.add name e computedErrors
                    | _ -> ()
                    Error e
                | Ok r -> Ok r
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In computed field %O: %s" name e.Message

        let computedFields = Map.map mapComputedField entity.ComputedFields

        let makeInheritance parent =
            let checkExpr = makeCheckHalfExpr (fun ref -> Map.find ref entities) entity
            { Parent = parent
              CheckExpr = checkExpr
            }

        let subEntityParseExpr =
            let getName (ref : ResolvedEntityRef, entity : HalfResolvedEntity) =
                if entity.Source.IsAbstract then
                    None
                else
                    let json = JToken.FromObject ref |> SQL.VJson |> SQL.VEValue
                    Some (entity, json)

            let column = SQL.VEColumn { table = None; name = sqlFunSubEntity }
            let childrenEntities = entity.Children |> Map.keys |> Seq.map (fun ref -> (ref, Map.tryFind ref entities |> Option.get))
            let allEntities = Seq.append (Seq.singleton (entityRef, entity)) childrenEntities
            let options = allEntities |> Seq.mapMaybe getName |> Seq.toArray

            let compileTag (entity : HalfResolvedEntity) =
                SQL.VEBinaryOp (column, SQL.BOEq, SQL.VEValue (SQL.VString entity.TypeName))
            composeExhaustingIf compileTag options

        // Ideally we would use `tempEntity` for it too, but the code for `resolveRelatedExpr` is already here, so...
        let mapCheckConstraint name constr =
            try
                checkName name
                resolveCheckConstraint Set.empty entityRef entity name constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In check constraint %O: %s" name e.Message
        let checkConstraints = Map.map mapCheckConstraint entity.Source.CheckConstraints

        try
            checkConstraints |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Check constraint names clash (first %i characters): %s" hashNameLength msg

        let tempEntity =
            { ColumnFields = entity.ColumnFields
              ComputedFields = computedFields
              UniqueConstraints = Map.empty
              CheckConstraints = checkConstraints
              Indexes = Map.empty
              MainField = entity.Source.MainField
              ForbidExternalReferences = entity.Source.ForbidExternalReferences
              ForbidTriggers = entity.Source.ForbidTriggers
              TriggersMigration = entity.Source.TriggersMigration
              IsHidden = entity.Source.IsHidden
              Inheritance = Option.map makeInheritance entity.Source.Parent
              SubEntityParseExpr = subEntityParseExpr
              Children = entity.Children
              Root = entity.Root
              TypeName = entity.TypeName
              IsAbstract = entity.Source.IsAbstract
              IsFrozen = entity.Source.IsFrozen
              HashName = makeHashName entityRef.name
            } : ResolvedEntity

        let mapUniqueConstraint name constr =
            try
                checkName name
                resolveUniqueConstraint tempEntity name constr
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In unique constraint %O: %s" name e.Message
        let uniqueConstraints = Map.map mapUniqueConstraint entity.Source.UniqueConstraints

        try
            uniqueConstraints |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Unique constraint names clash (first %i characters): %s" hashNameLength msg

        let mapIndex name index =
            try
                checkName name
                resolveIndex entityRef tempEntity name index
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In index %O: %s" name e.Message
        let indexes = Map.map mapIndex entity.Source.Indexes

        try
            indexes |> Map.values |> Seq.map (fun c -> c.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Index names clash (first %i characters): %s" hashNameLength msg

        let ret =
            { tempEntity with
                  UniqueConstraints = uniqueConstraints
                  Indexes = indexes
            }
        let errors =
            { ComputedFields = computedErrors
            }
        (errors, ret)

    let resolveSchema (schemaName : SchemaName) (schema : SourceSchema) : ErroredSchema * ResolvedSchema =
        let mutable roots = Set.empty
        let mutable errors = Map.empty

        let mapEntity name entity =
            let ref = { schema = schemaName; name = name }
            let halfEntity = Map.find ref entities
            try
                let (entityErrors, entity) = resolveEntity ref halfEntity
                if Option.isNone entity.Inheritance then
                    roots <- Set.add name roots
                if not (Map.isEmpty entityErrors.ComputedFields) then
                    errors <- Map.add name entityErrors errors
                entity
            with
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In entity %O: %s" name e.Message

        let entities = schema.Entities |> Map.map mapEntity

        try
            entities |> Map.values |> Seq.map (fun ent -> ent.HashName) |> Set.ofSeqUnique |> ignore
        with
        | Failure msg -> raisef ResolveLayoutException "Entity names hash clash: %s" msg

        let ret =
            { Entities = entities
              Roots = roots
            }
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
            | :? ResolveLayoutException as e -> raisefWithInner ResolveLayoutException e.InnerException "In schema %O: %s" name e.Message

        let ret =
            { Schemas = Map.map mapSchema layout.Schemas
            } : Layout
        (errors, ret)

    member this.ResolveLayout () = resolveLayout ()

//
// PHASE 3: Building cases for virtual computed fields.
//

type private HalfVirtualFieldCase =
    { Case : VirtualFieldCase
      Entities : Set<ResolvedEntityRef> // includes all children for a given case
    }

type private Phase3Resolver (layout : Layout) =
    let mutable cachedComputedFields : Map<ResolvedFieldRef, ResolvedComputedField> = Map.empty
    let mutable cachedCaseExpressions : Map<ResolvedFieldRef, HalfVirtualFieldCase array> = Map.empty

    let rec resolveMyVirtualCases (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : HalfVirtualFieldCase array =
        let childVirtualCases = findVirtualChildren entity fieldRef.name |> Seq.toArray
        let handledChildren = childVirtualCases |> Seq.map (fun c -> c.Entities) |> Set.unionMany

        let getName (ref, entity : ResolvedEntity) =
            if entity.IsAbstract || Set.contains ref handledChildren then
                None
            else
                Some (ref, entity.TypeName |> SQL.VString |> SQL.VEValue)

        let childrenEntities = entity.Children |> Map.keys |> Seq.map (fun ref -> (ref, layout.FindEntity ref |> Option.get))
        let allEntities = Seq.append (Seq.singleton (fieldRef.entity, entity)) childrenEntities
        let options = allEntities |> Seq.mapMaybe getName |> Seq.toArray

        if Array.isEmpty options then
            childVirtualCases
        else
            let checkExpr =
                if Array.length options = 1 then
                    let (_, entityValue) = options.[0]
                    SQL.VEBinaryOp (subEntityColumn, SQL.BOEq, entityValue)
                else
                    SQL.VEIn (subEntityColumn, Array.map snd options)
            let myCase =
                { Check = checkExpr
                  Expression = comp.Expression
                  Ref = fieldRef
                }
            let myHalf =
                { Case = myCase
                  Entities = options |> Seq.map fst |> Set.ofSeq
                }
            Array.append childVirtualCases [|myHalf|]

    and resolveVirtualCases (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : HalfVirtualFieldCase array =
        match Map.tryFind fieldRef cachedCaseExpressions with
        | Some cached -> cached
        | None ->
            let cases = resolveMyVirtualCases entity fieldRef comp
            cachedCaseExpressions <- Map.add fieldRef cases cachedCaseExpressions
            cases

    and findVirtualChildren (entity : ResolvedEntity) (fieldName : FieldName) : HalfVirtualFieldCase seq =
        entity.Children |> Map.toSeq |> Seq.collect (fun (ref, child) -> findVirtualChild fieldName ref child)

    and findVirtualChild (fieldName : FieldName) (entityRef : ResolvedEntityRef) (child : ChildEntity) : HalfVirtualFieldCase seq =
        if not child.Direct then
            Seq.empty
        else
            let childEntity = layout.FindEntity entityRef |> Option.get
            let childField = Map.find fieldName childEntity.ComputedFields
            match childField with
            | Ok f when Option.isNone f.InheritedFrom ->
                resolveVirtualCases childEntity { entity = entityRef; name = fieldName } f |> Array.toSeq
            | _ -> findVirtualChildren childEntity fieldName

    let resolveOneComputedField (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        match comp.VirtualCases with
        | None -> comp
        | Some _ ->
            let cases = resolveVirtualCases entity fieldRef comp |> Array.map (fun c -> c.Case)
            let getUsedSchemas (case : VirtualFieldCase) =
                let entity = layout.FindEntity case.Ref.entity |> Option.get
                let field = Map.find case.Ref.name entity.ComputedFields |> Result.get
                field.UsedSchemas
            // Add all fields used by virtual children as used fields for this field.
            let newUsed = cases |> Seq.map getUsedSchemas |> Seq.fold mergeUsedSchemas comp.UsedSchemas
            { comp with VirtualCases = Some cases; UsedSchemas = newUsed }

    let resolveComputedField (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (comp : ResolvedComputedField) : ResolvedComputedField =
        match Map.tryFind fieldRef cachedComputedFields with
        | Some f -> f
        | None ->
            let newField =
                match comp.InheritedFrom with
                | None -> resolveOneComputedField entity fieldRef comp
                | Some parentRef ->
                    let parentFieldRef = { fieldRef with entity = parentRef }
                    let parentField =
                        match Map.tryFind parentFieldRef cachedComputedFields with
                        | Some f -> f
                        | None ->
                            let parentEntity = layout.FindEntity parentRef |> Option.get
                            let parentField = Map.find parentFieldRef.name parentEntity.ComputedFields |> Result.get
                            let field = resolveOneComputedField parentEntity parentFieldRef parentField
                            cachedComputedFields <- Map.add fieldRef field cachedComputedFields
                            field
                    { parentField with InheritedFrom = comp.InheritedFrom }
            cachedComputedFields <- Map.add fieldRef newField cachedComputedFields
            newField

    let resolveEntity (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : ResolvedEntity =
        let mapComputedField name = function
            | Ok comp -> Ok <| resolveComputedField entity { entity = entityRef; name = name } comp
            | Error err -> Error err
        { entity with ComputedFields = Map.map mapComputedField entity.ComputedFields }

    let resolveSchema (schemaName : SchemaName) (schema : ResolvedSchema) : ResolvedSchema =
        let mapEntity name entity = resolveEntity { schema = schemaName; name = name } entity
        { Entities = Map.map mapEntity schema.Entities
          Roots = schema.Roots
        }

    let resolveLayout () : Layout =
        { Schemas = Map.map resolveSchema layout.Schemas
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
