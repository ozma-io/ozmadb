module FunWithFlags.FunDB.Layout.Domain

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

// Field domains, that is, all possible values for a field considering check constraints.

// Right now all domains are SQL expressions with `value` and `pun` columns. This may change in future.
type DomainExpr =
    { Query : Query<SQL.SelectExpr>
      UsedSchemas : UsedSchemas
      Hash : string
    }

type FieldDomain =
    { Generic : DomainExpr
      RowSpecific : DomainExpr option
    }

type EntityDomains =
    { Fields : Map<FieldName, FieldDomain>
    }

type SchemaDomains =
    { Entities : Map<EntityName, EntityDomains>
    }

type LayoutDomains =
    { Schemas : Map<SchemaName, SchemaDomains>
    }

let findDomainForField (layout : Layout) (fieldRef : ResolvedFieldRef) (domains : LayoutDomains) : FieldDomain option =
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
let private hasLocalDependencies (ref : ResolvedFieldRef) (usedSchemas : UsedSchemas) : bool =
    let localEntity = usedSchemas |> Map.find ref.Entity.Schema |> Map.find ref.Entity.Name
    // We always have at least one field in used local fields: the reference itself. Hence we just check that size is larger than 1.
    Set.count localEntity > 1

let private makeResultExpr (boundEntityRef : ResolvedEntityRef) (fieldRef : FieldRef) : ResolvedFieldExpr =
    let boundInfo =
        { Ref =
            { Entity = boundEntityRef
              Name = fieldRef.Name
            }
          Immediate = true
          Path = [||]
        } : BoundFieldMeta
    let fieldInfo =
        { Bound = Some boundInfo
          FromEntityId = localExprFromEntityId
          ForceSQLName = None
        } : FieldMeta
    let column =
        { Ref = VRColumn fieldRef
          Path = [||]
        } : LinkedFieldRef
    let resultColumn =
        { Ref = column
          Extra = ObjectMap.singleton fieldInfo
        }
    FERef resultColumn

let private makeResultColumn (name : FieldName option) (expr : ResolvedFieldExpr) : ResolvedQueryResult =
    let col =
        { Attributes = Map.empty
          Result = expr
          Alias = name
        }
    QRExpr col

let domainValueName = FunQLName "value"
let sqlDomainValueName = compileName domainValueName

let domainPunName = FunQLName "pun"
let sqlDomainPunName = compileName domainPunName

let rowName = FunQLName "row"
let rowEntityRef : EntityRef = { Schema = None; Name = rowName }
let referencedName = FunQLName "referenced"
let referencedEntityRef : EntityRef = { Schema = None; Name = referencedName }

// Generic check is the one that only has has references to one field, itself of reference type.
// For example, `user_id=>account_id=>balance > 0`.
// Convert this to just `account_id=>balance > 0`, so that we can use it SELECT for referenced entity.
//
// Row-specific check is the one that has both references to the field we build domain for, as well as other local fields.
// We split them into references to two separate entities: `row` and `referenced`.
let private renameDomainCheck (refEntityRef : ResolvedEntityRef) (refFieldName : FieldName) (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
    let convertRef : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
        | { Ref = { Ref = VRColumn { Name = fieldName }; Path = path }; Extra = extra } ->
            let fieldInfo = ObjectMap.findType<FieldMeta> extra
            let boundInfo = Option.get fieldInfo.Bound
            if fieldName = refFieldName then
                // Referenced entity.
                if Array.isEmpty path then
                    let newBoundInfo =
                        { Ref = { Entity = refEntityRef; Name = funId }
                          Immediate = true
                          Path = [||]
                        } : BoundFieldMeta
                    let newFieldInfo =
                        { Bound = Some newBoundInfo
                          FromEntityId = fieldInfo.FromEntityId
                          ForceSQLName = None
                        } : FieldMeta
                    { Ref = { Ref = VRColumn { Entity = Some referencedEntityRef; Name = funId }; Path = [||] }; Extra = ObjectMap.add newFieldInfo extra }
                else
                    let name = path.[0]
                    let newBoundInfo =
                        { Ref = { Entity = refEntityRef; Name = name }
                          Immediate = true
                          Path = Array.skip 1 boundInfo.Path
                        } : BoundFieldMeta
                    let newFieldInfo =
                        { Bound = Some newBoundInfo
                          FromEntityId = fieldInfo.FromEntityId
                          ForceSQLName = None
                        } : FieldMeta
                    { Ref = { Ref = VRColumn { Entity = Some referencedEntityRef; Name = name }; Path = Array.skip 1 path }; Extra = ObjectMap.add newFieldInfo extra }
            else
                { Ref = { Ref = VRColumn { Entity = Some rowEntityRef; Name = fieldName }; Path = path }; Extra = extra }
        | ref -> failwithf "Impossible reference: %O" ref
    let convertQuery query = failwithf "Impossible query: %O" query
    mapFieldExpr (idFieldExprMapper convertRef convertQuery) expr

let private queryHash (expr : SQL.SelectExpr) : string =
    expr |> string |> Hash.sha1OfString |> String.hexBytes

let private compileReferenceOptionsSelectFrom (layout : Layout) (refEntityRef : ResolvedEntityRef) (arguments : QueryArguments) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : UsedSchemas * Query<SQL.SelectExpr> =
    let idExpr = makeResultExpr refEntityRef { Entity = Some referencedEntityRef; Name = funId }
    let mainExpr = makeResultExpr refEntityRef { Entity = Some referencedEntityRef; Name = funMain }
    let orderByMain =
        { emptyOrderLimitClause with
              OrderBy = [| (Asc, mainExpr) |]
        }
    let single =
        { Attributes = Map.empty
          Results = [| makeResultColumn (Some domainValueName) idExpr; makeResultColumn (Some domainPunName) mainExpr |]
          From = Some from
          Where = where
          GroupBy = [||]
          OrderLimit = orderByMain
          Extra = ObjectMap.empty
        }
    let select =
        { CTEs = None
          Tree = SSelect single
          Extra = ObjectMap.empty
        }
    let (info, expr) = compileSelectExpr layout arguments select
    let query =
        { Expression = expr
          Arguments = info.Arguments
        }
    (info.UsedSchemas, query)

let private compileGenericReferenceOptionsSelect (layout : Layout) (refEntityRef : ResolvedEntityRef) (where : ResolvedFieldExpr option) : UsedSchemas * Query<SQL.SelectExpr> =
    let from = FEntity (Some referencedName, relaxEntityRef refEntityRef)
    compileReferenceOptionsSelectFrom layout refEntityRef emptyArguments from where

let private compileRowSpecificReferenceOptionsSelect (layout : Layout) (entityRef : ResolvedEntityRef) (refEntityRef : ResolvedEntityRef) (extraWhere : ResolvedFieldExpr option) : UsedSchemas * Query<SQL.SelectExpr> =
    let rowFrom = FEntity (Some rowName, relaxEntityRef entityRef)
    let refFrom = FEntity (Some referencedName, relaxEntityRef refEntityRef)
    // This could be rewritten to use CROSS JOIN.
    let join =
        { Type = Outer
          A = rowFrom
          B = refFrom
          Condition = FEValue (FBool true)
        }
    let argumentInfo =
        { ArgType = FTReference entityRef
          Optional = false
        } : ResolvedArgument
    let placeholder = PLocal funId
    let (argId, arguments) = addArgument placeholder argumentInfo emptyArguments

    let idCol : ResolvedFieldExpr = FERef { Ref = { Ref = VRColumn { Entity = Some rowEntityRef; Name = funId }; Path = [||] }; Extra = ObjectMap.empty }
    let argRef : ResolvedFieldExpr = FERef { Ref = { Ref = VRPlaceholder placeholder; Path = [||] }; Extra = ObjectMap.empty }
    let where = FEBinaryOp (idCol, BOEq, argRef)
    let where =
        match extraWhere with
        | None -> where
        | Some extra -> FEAnd (where, extra)
    compileReferenceOptionsSelectFrom layout refEntityRef arguments (FJoin join) (Some where)

// For now, we only build domains based on immediate check constraints.
// For example, for check constraint `user_id=>account_id=>balance > 0` we restrict `user_id` based on that.
// However, we could also restrict `account_id`. This is to be implemented yet.
// We, however, also build row-specific checks for constraints such as `user_id=>account_id=>balance > my_balance`.
type private DomainsBuilder (layout : Layout) =
    let mutable fullSelectsCache : Map<ResolvedEntityRef, DomainExpr> = Map.empty

    let buildFullSelect (refEntityRef : ResolvedEntityRef) =
        match Map.tryFind refEntityRef fullSelectsCache with
        | Some cached -> cached
        | None ->
            let (usedSchemas, query) = compileGenericReferenceOptionsSelect layout refEntityRef None
            let ret =
                { Query = query
                  UsedSchemas = usedSchemas
                  Hash = queryHash query.Expression
                }
            fullSelectsCache <- Map.add refEntityRef ret fullSelectsCache
            ret

    let buildReferenceFieldDomain (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (refEntityRef : ResolvedEntityRef) : FieldDomain =
        let (rowSpecificChecks, genericChecks) =
            entity.CheckConstraints
            |> Map.values
            |> Seq.filter (fun constr -> isFieldUsed fieldRef constr.UsedSchemas)
            |> Seq.partition (fun constr -> hasLocalDependencies fieldRef constr.UsedSchemas)

        let mergeChecks (usedSchemas1, check1) (usedSchemas2, check2) = (mergeUsedSchemas usedSchemas1 usedSchemas2, FEAnd (check1, check2))
        let buildCheck (checks : ResolvedCheckConstraint seq) : UsedSchemas * ResolvedFieldExpr =
            checks |> Seq.map (fun constr -> (constr.UsedSchemas, renameDomainCheck refEntityRef fieldRef.Name constr.Expression)) |> Seq.fold1 mergeChecks

        let genericCheck =
            if Seq.isEmpty genericChecks then
                None
            else
                Some (buildCheck genericChecks)
        let genericExpr =
            match genericCheck with
            | None -> buildFullSelect refEntityRef
            | Some (usedSchemas, check) ->
                let (selectUsedSchemas, query) = compileGenericReferenceOptionsSelect layout refEntityRef (Some check)
                { Query = query
                  UsedSchemas = mergeUsedSchemas usedSchemas selectUsedSchemas
                  Hash = queryHash query.Expression
                }
        let rowSpecificExpr =
            if Seq.isEmpty rowSpecificChecks then
                None
            else
                let rowSpecificPair = buildCheck rowSpecificChecks
                let (usedSchemas, fullCheck) =
                    match genericCheck with
                    | None -> rowSpecificPair
                    | Some pair -> mergeChecks rowSpecificPair pair
                let (selectUsedSchemas, query) = compileRowSpecificReferenceOptionsSelect layout fieldRef.Entity refEntityRef (Some fullCheck)
                Some
                    { Query = query
                      UsedSchemas = mergeUsedSchemas usedSchemas selectUsedSchemas
                      Hash = queryHash query.Expression
                    }

        { Generic = genericExpr
          RowSpecific = rowSpecificExpr
        }

    let buildFieldDomain (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (field : ResolvedColumnField) : FieldDomain option =
        match field.FieldType with
        | FTReference refEntityRef -> Some <| buildReferenceFieldDomain entity fieldRef refEntityRef
        // We can also potentially make domains for enums and booleans.
        | _ -> None

    let buildEntityDomains (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : EntityDomains option =
        let mapField name (field : ResolvedColumnField) =
            if Option.isSome field.InheritedFrom then
                None
            else
                let ref = { Entity = entityRef; Name = name }
                buildFieldDomain entity ref field
        let res = entity.ColumnFields |> Map.mapMaybe mapField
        if Map.isEmpty res then
            None
        else
            Some { Fields = res }

    let buildSchemaDomains (schemaName : SchemaName) (schema : ResolvedSchema) : SchemaDomains option =
        let mapEntity name entity =
            if entity.IsHidden then
                None
            else
                let ref = { Schema = schemaName; Name = name }
                buildEntityDomains ref entity
        let res = schema.Entities |> Map.mapMaybe mapEntity
        if Map.isEmpty res then
            None
        else
            Some { Entities = res }

    let buildLayoutDomains () : LayoutDomains =
        let res = layout.Schemas |> Map.mapMaybe buildSchemaDomains
        { Schemas = res
        }

    member this.BuildLayoutDomains () = buildLayoutDomains ()

let buildLayoutDomains (layout : Layout) : LayoutDomains =
    let builder = DomainsBuilder layout
    builder.BuildLayoutDomains ()