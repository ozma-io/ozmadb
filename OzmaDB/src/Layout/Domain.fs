module OzmaDB.Layout.Domain

open FSharpPlus

open OzmaDB.OzmaUtils
open OzmaDB.Exception
open OzmaDB.OzmaQL.AST
open OzmaDB.OzmaQL.Arguments
open OzmaDB.OzmaQL.Resolve
open OzmaDB.OzmaQL.UsedReferences
open OzmaDB.OzmaQL.Compile
open OzmaDB.Layout.Types

module SQL = OzmaDB.SQL.AST

type LayoutDomainException(message: string, innerException: exn, isUserException: bool) =
    inherit UserException(message, innerException, isUserException)

    new(message: string, innerException: exn) =
        LayoutDomainException(message, innerException, isUserException innerException)

    new(message: string) = LayoutDomainException(message, null, true)

// Field domains, that is, all possible values for a field considering check constraints.

// Right now all domains are SQL expressions with `value` and `pun` columns. This may change in future.
type DomainExpr =
    { Query: Query<SQL.SelectExpr>
      UsedDatabase: FlatUsedDatabase
      PunValueType: SQL.SimpleValueType
      Hash: string }

type FieldDomain =
    { Generic: DomainExpr
      RowSpecific: DomainExpr option }

type EntityDomains = { Fields: Map<FieldName, FieldDomain> }

type SchemaDomains =
    { Entities: Map<EntityName, EntityDomains> }

type LayoutDomains =
    { Schemas: Map<SchemaName, SchemaDomains> }

let findDomainForField (layout: Layout) (fieldRef: ResolvedFieldRef) (domains: LayoutDomains) : FieldDomain option =
    let rootEntity =
        match layout.FindField fieldRef.Entity fieldRef.Name with
        | Some { Field = RColumnField col } -> Some <| Option.defaultValue fieldRef.Entity col.InheritedFrom
        | Some { Field = RComputedField comp } -> Some <| Option.defaultValue fieldRef.Entity comp.InheritedFrom
        | _ -> None

    match rootEntity with
    | None -> None
    | Some ent ->
        match Map.tryFind ent.Schema domains.Schemas with
        | None -> None
        | Some schema ->
            match Map.tryFind ent.Name schema.Entities with
            | None -> None
            | Some entity -> Map.tryFind fieldRef.Name entity.Fields

// If domain depends on values in local row (that is, will be different for different rows).
let private hasLocalDependencies (entityRef: ResolvedEntityRef) (usedDatabase: UsedDatabase) : bool =
    let localEntity = usedDatabase.FindEntity entityRef |> Option.get
    // We always have at least one field in used local fields: the reference itself. Hence we just check that size is larger than 1.
    Map.count localEntity.Fields > 1

let private makeResultColumn (name: FieldName option) (expr: ResolvedFieldExpr) : ResolvedQueryResult =
    let col =
        { Attributes = Map.empty
          Result = expr
          Alias = name }

    QRExpr col

let domainValueName = OzmaQLName "value"
let sqlDomainValueName = compileName domainValueName

let domainPunName = OzmaQLName "pun"
let sqlDomainPunName = compileName domainPunName

let rowName = OzmaQLName "row"
let rowEntityRef: EntityRef = { Schema = None; Name = rowName }
let referencedName = OzmaQLName "referenced"
let referencedEntityRef: EntityRef = { Schema = None; Name = referencedName }

// Generic check is the one that only has references to one field, itself of reference type.
// For example, `user_id=>account_id=>balance > 0`.
// Convert this to just `account_id=>balance > 0`, so that we can use it in SELECT for referenced entity.
//
// Row-specific check is the one that has both references to the field we build domain for, as well as other local fields.
// We split them into references to two separate entities: `row` and `referenced`.
let private renameDomainCheck
    (layout: Layout)
    (refEntityRef: ResolvedEntityRef)
    (refFieldName: FieldName)
    (expr: ResolvedFieldExpr)
    : ResolvedFieldExpr =
    let convertRef: LinkedBoundFieldRef -> LinkedBoundFieldRef =
        function
        | { Ref = { Ref = VRColumn { Name = fieldName }
                    Path = path }
            Extra = extra } as ref ->
            let info = ObjectMap.findType<FieldRefMeta> extra
            let columnInfo = Option.get info.Column
            let boundInfo = getFieldRefBoundColumn info

            if fieldName = refFieldName then
                // Referenced entity.
                if Array.isEmpty path then
                    let fieldRef =
                        { Entity = Some referencedEntityRef
                          Name = funId }
                        : FieldRef

                    let simpleMeta =
                        { simpleColumnMeta refEntityRef with
                            EntityId = columnInfo.EntityId }

                    makeColumnReference layout simpleMeta fieldRef
                else
                    let firstArrow = path.[0]
                    let firstEntityRef = info.Path.[0]

                    let simpleMeta =
                        { simpleColumnMeta firstEntityRef with
                            EntityId = columnInfo.EntityId
                            Path = Array.skip 1 path
                            PathEntities = Array.skip 1 info.Path }

                    let fieldRef =
                        { Entity = Some referencedEntityRef
                          Name = firstArrow.Name }
                        : FieldRef

                    makeColumnReference layout simpleMeta fieldRef
            else
                let lref =
                    { Ref =
                        VRColumn
                            { Entity = Some rowEntityRef
                              Name = fieldName }
                      Path = path
                      AsRoot = false }
                    : LinkedFieldRef

                { Ref = lref; Extra = extra }
        | ref -> failwithf "Impossible reference: %O" ref

    mapFieldExpr (onlyFieldExprMapper convertRef) expr

let private queryHash (expr: SQL.SelectExpr) : string =
    expr |> string |> Hash.sha1OfString |> String.hexBytes

let private compileReferenceOptionsSelectFrom
    (layout: Layout)
    (refEntityRef: ResolvedEntityRef)
    (arguments: QueryArguments)
    (from: ResolvedFromExpr)
    (isInner: bool)
    (where: ResolvedFieldExpr option)
    : UsedDatabase * Query<SQL.SelectExpr> =
    let exprMeta =
        { simpleColumnMeta refEntityRef with
            IsInner = isInner }

    let idExpr =
        makeSingleFieldExpr
            layout
            exprMeta
            { Entity = Some referencedEntityRef
              Name = funId }

    let mainExpr =
        makeSingleFieldExpr
            layout
            exprMeta
            { Entity = Some referencedEntityRef
              Name = funMain }

    let mainSortColumn =
        { Expr = mainExpr
          Order = None
          Nulls = None }

    let orderByMain =
        { emptyOrderLimitClause with
            OrderBy = [| mainSortColumn |] }

    let single =
        { emptySingleSelectExpr with
            Results =
                [| makeResultColumn (Some domainValueName) idExpr
                   makeResultColumn (Some domainPunName) mainExpr |]
            From = Some from
            Where = where
            OrderLimit = orderByMain }

    let select = selectExpr (SSelect single)

    let (info, expr) =
        try
            compileSelectExpr layout arguments select
        with e ->
            raisefWithInner LayoutDomainException e "While compiling %O" select

    let query =
        { Expression = expr
          Arguments = info.Arguments }

    (info.UsedDatabase, query)

let private compileGenericReferenceOptionsSelect
    (layout: Layout)
    (refEntityRef: ResolvedEntityRef)
    (where: ResolvedFieldExpr option)
    : UsedDatabase * Query<SQL.SelectExpr> =
    let from =
        FEntity
            { fromEntity (relaxEntityRef refEntityRef) with
                Alias = Some referencedName }

    compileReferenceOptionsSelectFrom layout refEntityRef emptyArguments from true where

let private compileRowSpecificReferenceOptionsSelect
    (layout: Layout)
    (fieldRef: ResolvedFieldRef)
    (refEntityRef: ResolvedEntityRef)
    (extraWhere: ResolvedFieldExpr option)
    : UsedDatabase * Query<SQL.SelectExpr> =
    let rowFrom =
        FEntity
            { fromEntity (relaxEntityRef fieldRef.Entity) with
                Alias = Some rowName }

    let refFrom =
        FEntity
            { fromEntity (relaxEntityRef refEntityRef) with
                Alias = Some referencedName }

    let join =
        { Type = Inner
          A = rowFrom
          B = refFrom
          Condition = FEValue(FBool true) }

    let argumentInfo = requiredArgument <| FTScalar(SFTReference(fieldRef.Entity, None))
    let placeholder = PLocal funId
    let (argId, arguments) = addArgument placeholder argumentInfo emptyArguments

    let idCol =
        resolvedRefFieldExpr
        <| VRColumn
            { Entity = Some rowEntityRef
              Name = funId }
        : ResolvedFieldExpr

    let argRef = resolvedRefFieldExpr <| VRArgument placeholder
    let where = FEBinaryOp(idCol, BOEq, argRef)
    let where = Option.addWith (curry FEAnd) where extraWhere
    compileReferenceOptionsSelectFrom layout refEntityRef arguments (FJoin join) false (Some where)

type private ChildCheckTree =
    { Entity: ResolvedEntityRef
      Checks: ResolvedCheckConstraint seq
      Children: ChildCheckTrees }

and private ChildCheckTrees = ChildCheckTree seq

let rec private filterChildCheckTree (f: ResolvedCheckConstraint -> bool) (tree: ChildCheckTree) : ChildCheckTree =
    { Entity = tree.Entity
      Checks = Seq.filter f tree.Checks
      Children = filterChildCheckTrees f tree.Children }

and private filterChildCheckTrees (f: ResolvedCheckConstraint -> bool) = Seq.map (filterChildCheckTree f)

// For now, we only build domains based on immediate check constraints.
// For example, for check constraint `user_id=>account_id=>balance > 0` we restrict `user_id` based on that.
// However, we could also restrict `account_id`. This is to be implemented yet.
// We, however, also build row-specific checks for constraints such as `user_id=>account_id=>balance > my_balance`.
type private DomainsBuilder(layout: Layout) =
    let mutable fullSelectsCache: Map<ResolvedEntityRef, DomainExpr> = Map.empty

    let punValueType (refEntityRef: ResolvedEntityRef) =
        let entity = layout.FindEntity refEntityRef |> Option.get
        let mainField = entity.FindField entity.MainField |> Option.get
        mainField.Field |> resolvedFieldType |> compileFieldType

    let buildFullSelect (refEntityRef: ResolvedEntityRef) =
        match Map.tryFind refEntityRef fullSelectsCache with
        | Some cached -> cached
        | None ->
            let (usedDatabase, query) =
                compileGenericReferenceOptionsSelect layout refEntityRef None

            let ret =
                { Query = query
                  UsedDatabase = flattenUsedDatabase layout usedDatabase
                  Hash = queryHash query.Expression
                  PunValueType = punValueType refEntityRef }

            fullSelectsCache <- Map.add refEntityRef ret fullSelectsCache
            ret

    let rec getParentEntityChecks (entityRef: ResolvedEntityRef) (fieldName: FieldName) (entity: ResolvedEntity) =
        seq {
            yield!
                entity.CheckConstraints
                |> Map.values
                |> Seq.map (fun constr -> (entityRef, constr))

            match entity.Parent with
            | None -> ()
            | Some parentRef ->
                let parentEntity = layout.FindEntity parentRef |> Option.get

                if Map.containsKey fieldName parentEntity.ColumnFields then
                    yield! getParentEntityChecks parentRef fieldName parentEntity
        }

    let buildReferenceFieldDomain
        (entity: ResolvedEntity)
        (fieldRef: ResolvedFieldRef)
        (refEntityRef: ResolvedEntityRef)
        : FieldDomain =
        let (rowSpecificChecks, genericChecks) =
            getParentEntityChecks fieldRef.Entity fieldRef.Name entity
            |> Seq.filter (fun (entityRef, constr) ->
                constr.UsedDatabase.FindField entityRef fieldRef.Name |> Option.isSome)
            |> Seq.partition (fun (entityRef, constr) -> hasLocalDependencies entityRef constr.UsedDatabase)

        let mergeChecks
            (usedDatabase1: UsedDatabase, check1: ResolvedFieldExpr)
            (usedDatabase2: UsedDatabase, check2: ResolvedFieldExpr)
            =
            (unionUsedDatabases usedDatabase1 usedDatabase2, FEAnd(check1, check2))

        let buildCheck (checks: (ResolvedEntityRef * ResolvedCheckConstraint) seq) : UsedDatabase * ResolvedFieldExpr =
            checks
            |> Seq.map (fun (entityRef, constr) ->
                (constr.UsedDatabase, renameDomainCheck layout refEntityRef fieldRef.Name constr.Expression))
            |> Seq.fold1 mergeChecks

        let genericCheck =
            if Seq.isEmpty genericChecks then
                None
            else
                Some(buildCheck genericChecks)

        let genericExpr =
            match genericCheck with
            | None -> buildFullSelect refEntityRef
            | Some(usedDatabase, check) ->
                let (selectUsedDatabase, query) =
                    compileGenericReferenceOptionsSelect layout refEntityRef (Some check)

                { Query = query
                  UsedDatabase = flattenUsedDatabase layout <| unionUsedDatabases usedDatabase selectUsedDatabase
                  Hash = queryHash query.Expression
                  PunValueType = punValueType refEntityRef }

        let rowSpecificExpr =
            if Seq.isEmpty rowSpecificChecks then
                None
            else
                let rowSpecificPair = buildCheck rowSpecificChecks

                let (usedDatabase, fullCheck) =
                    match genericCheck with
                    | None -> rowSpecificPair
                    | Some pair -> mergeChecks rowSpecificPair pair

                let (selectUsedDatabase, query) =
                    compileRowSpecificReferenceOptionsSelect layout fieldRef refEntityRef (Some fullCheck)

                Some
                    { Query = query
                      UsedDatabase = flattenUsedDatabase layout <| unionUsedDatabases usedDatabase selectUsedDatabase
                      Hash = queryHash query.Expression
                      PunValueType = punValueType refEntityRef }

        { Generic = genericExpr
          RowSpecific = rowSpecificExpr }

    let buildFieldDomain
        (entity: ResolvedEntity)
        (fieldRef: ResolvedFieldRef)
        (field: ResolvedColumnField)
        : FieldDomain option =
        match field.FieldType with
        | FTScalar(SFTReference(refEntityRef, opts)) -> Some <| buildReferenceFieldDomain entity fieldRef refEntityRef
        // We can also potentially make domains for enums and booleans.
        | _ -> None

    let buildEntityDomains (entityRef: ResolvedEntityRef) (entity: ResolvedEntity) : EntityDomains option =
        let mapField name (field: ResolvedColumnField) =
            if Option.isSome field.InheritedFrom then
                None
            else
                let ref = { Entity = entityRef; Name = name }

                try
                    buildFieldDomain entity ref field
                with e ->
                    raisefWithInner LayoutDomainException e "In field %O" name

        let res = entity.ColumnFields |> Map.mapMaybe mapField
        if Map.isEmpty res then None else Some { Fields = res }

    let buildSchemaDomains (schemaName: SchemaName) (schema: ResolvedSchema) : SchemaDomains option =
        let mapEntity name entity =
            if entity.IsHidden then
                None
            else
                let ref = { Schema = schemaName; Name = name }

                try
                    buildEntityDomains ref entity
                with e ->
                    raisefWithInner LayoutDomainException e "In entity %O" name

        let res = schema.Entities |> Map.mapMaybe mapEntity
        if Map.isEmpty res then None else Some { Entities = res }

    let buildLayoutDomains () : LayoutDomains =
        let mapSchema name schema =
            try
                buildSchemaDomains name schema
            with e ->
                raisefWithInner LayoutDomainException e "In schema %O" name

        let res = layout.Schemas |> Map.mapMaybe mapSchema
        { Schemas = res }

    member this.BuildLayoutDomains() = buildLayoutDomains ()

    member this.BuildSingleLayoutDomain(fieldRef: ResolvedFieldRef) : FieldDomain option =
        let entity = layout.FindEntity fieldRef.Entity |> Option.get
        let field = Map.find fieldRef.Name entity.ColumnFields
        buildFieldDomain entity fieldRef field

let buildLayoutDomains (layout: Layout) : LayoutDomains =
    let builder = DomainsBuilder layout
    builder.BuildLayoutDomains()

let buildSingleLayoutDomain (layout: Layout) (fieldRef: ResolvedFieldRef) : FieldDomain option =
    let builder = DomainsBuilder layout
    builder.BuildSingleLayoutDomain fieldRef
