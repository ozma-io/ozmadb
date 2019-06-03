module FunWithFlags.FunDB.Permissions.Flatten

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types

// Actually stringified expression, used for deduplication.
type RestrictionKey = string
// Logically OR'ed expressions from different roles.
type RestrictionsAny = Map<RestrictionKey, Restriction>
// Logically AND'ed expressions from one role.
type RestrictionsAll = Map<RestrictionKey, Restriction>

[<NoComparison>]
type AllowedOperation =
    | OAllowed
    | OFiltered of RestrictionsAll

type FieldOperations = Map<FieldName, AllowedOperation>

[<NoComparison>]
type FlatAllowedEntity =
    { update : FieldOperations
      // None means can't insert at all. Some Set.empty means can insert only Id.
      insert : Set<FieldName> option
      // None means can't select at all. Some Map.empty means can select only Id.
      select : FieldOperations option
      delete : AllowedOperation option
      check : AllowedOperation option
    }

[<NoComparison>]
type FlatAllowedSchema =
    { entities : Map<EntityName, FlatAllowedEntity>
    }

[<NoComparison>]
type FlatAllowedDatabase =
    { schemas : Map<SchemaName, FlatAllowedSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
                | None -> None
                | Some schema -> Map.tryFind entity.name schema.entities

[<NoComparison>]
type FlatRole =
    { permissions : FlatAllowedDatabase
    } with
        member this.FindEntity entity = this.permissions.FindEntity entity

[<NoComparison>]
type FlatPermissionsSchema =
    { roles : Map<RoleName, FlatRole>
    }

[<NoComparison>]
type FlatPermissions =
    { schemas : Map<SchemaName, FlatPermissionsSchema>
    } with
        member this.FindRole (ref : ResolvedRoleRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.roles

type private HalfFieldRestrictions = Map<FieldName, RestrictionsAny>

[<NoComparison>]
type private HalfFlatAllowedEntity =
    { insert : Set<FieldName> option
      check : RestrictionsAny
      select : HalfFieldRestrictions option
      update : HalfFieldRestrictions
      delete : RestrictionsAny
    }

[<NoComparison>]
type private HalfFlatAllowedSchema =
    { entities : Map<EntityName, HalfFlatAllowedEntity>
    }

[<NoComparison>]
type private HalfFlatAllowedDatabase =
    { schemas : Map<SchemaName, HalfFlatAllowedSchema>
    }

let private restrictionAnd (a : Restriction) (b : Restriction) =
    let e =
        match (a.expression, b.expression) with
        | (FEValue (FBool true), e)
        | (e, FEValue (FBool true)) -> e
        | (e1, e2) -> FEOr (e1, e2)
    { expression = e
      globalArguments = Set.union a.globalArguments b.globalArguments
    }

let private restrictionOr (a : Restriction) (b : Restriction) =
    let e =
        match (a.expression, b.expression) with
        | (FEValue (FBool false), e)
        | (e, FEValue (FBool false)) -> e
        | (e1, e2) -> FEOr (e1, e2)
    { expression = e
      globalArguments = Set.union a.globalArguments b.globalArguments
    }

let mergeAllowedOperation (a : AllowedOperation) (b : AllowedOperation) =
    match (a, b) with
    | (OAllowed, OAllowed) -> OAllowed
    | (OFiltered f, OAllowed)
    | (OAllowed, OFiltered f) -> OFiltered f
    | (OFiltered f1, OFiltered f2) -> OFiltered <| Map.union f1 f2

let allowedOperationRestriction : AllowedOperation -> Restriction option = function
    | OAllowed -> None
    | OFiltered f -> f |> Map.values |> Seq.fold1 restrictionAnd |> Some

let private mergeRestrictionsAny (restrs : RestrictionsAny) =
    match restrs |> Map.values |> Seq.fold1 restrictionOr with
    | { expression = FEValue (FBool true) } -> OAllowed
    | restr -> OFiltered <| Map.singleton (restr.ToFunQLString()) restr

let private mapSingleRestriction (restrs : RestrictionsAny) =
    if Map.isEmpty restrs then
        None
    else
        Some <| mergeRestrictionsAny restrs

let private finalizeAllowedEntity (entity : HalfFlatAllowedEntity) : FlatAllowedEntity =
    let merge = Map.map (fun name -> mergeRestrictionsAny)
    { insert = entity.insert
      select = Option.map merge entity.select
      update = merge entity.update
      check = mapSingleRestriction entity.check
      delete = mapSingleRestriction entity.delete
    }

let private finalizeAllowedSchema (schema : HalfFlatAllowedSchema) : FlatAllowedSchema =
    { entities = Map.map (fun name -> finalizeAllowedEntity) schema.entities
    }

let private finalizeAllowedDatabase (db : HalfFlatAllowedDatabase) : FlatAllowedDatabase =
    { schemas = Map.map (fun name -> finalizeAllowedSchema) db.schemas
    }

let private mergeHalfFieldRestrictions = Map.unionWith (fun name -> Map.union)

let private mergeAllowedEntity (a : HalfFlatAllowedEntity) (b : HalfFlatAllowedEntity) : HalfFlatAllowedEntity =
    { insert = Option.unionWith Set.union a.insert b.insert
      select = Option.unionWith mergeHalfFieldRestrictions a.select b.select
      update = mergeHalfFieldRestrictions a.update b.update
      check = Map.union a.check b.check
      delete = Map.union a.delete b.delete
    }

let private mergeAllowedSchema (a : HalfFlatAllowedSchema) (b : HalfFlatAllowedSchema) : HalfFlatAllowedSchema =
    { entities = Map.unionWith (fun name -> mergeAllowedEntity) a.entities b.entities
    }

let private mergeAllowedDatabase (a : HalfFlatAllowedDatabase) (b : HalfFlatAllowedDatabase) : HalfFlatAllowedDatabase =
    { schemas = Map.unionWith (fun name -> mergeAllowedSchema) a.schemas b.schemas
    }

let private flattenAllowedEntity (ent : AllowedEntity) : HalfFlatAllowedEntity =
    let flattenRestrictions = Option.map (fun (where : Restriction) -> (where.ToFunQLString()), where)

    let makeFields (accessor : AllowedField -> Restriction option) (entityExpr : Restriction) =
        let entityStr = entityExpr.ToFunQLString()

        let mapField field =
            match accessor field with
            | None -> None
            | Some fieldExpr ->
                let fieldStr = fieldExpr.ToFunQLString()
                if entityStr = fieldStr then
                    Some fieldExpr
                else
                    Some <| restrictionAnd entityExpr fieldExpr

        ent.fields |> Map.mapMaybe (fun name -> mapField)

    let makeRestriction (e : Restriction) = Map.singleton (e.ToFunQLString()) e
    let makeRestrictions = Map.map (fun name -> makeRestriction)
    let foldRestrictionsAll restrs =
        let expr = restrs |> Map.values |> Seq.fold1 restrictionAnd
        Map.singleton (expr.ToFunQLString()) expr

    let mergeEntries = Map.intersectWith (fun name -> Map.union)

    let change = ent.fields |> Map.toSeq |> Seq.filter (fun (name, field) -> field.change) |> Seq.map fst |> Set.ofSeq

    let selectFields = Option.map (makeFields (fun x -> x.select)) ent.select
    let select = selectFields |> Option.map makeRestrictions
    let selectWithKeys = Option.map makeRestrictions selectFields

    let update =
        // Merge SELECT, check should be present.
        match (selectWithKeys, ent.update, ent.check) with
        | (Some ent, Some upd, Some _) ->
            let updRestriction = makeRestriction upd
            let mapOne name restrs =
                if Set.contains name change then
                    Map.union restrs updRestriction |> foldRestrictionsAll |> Some
                else
                    None
            ent |> Map.mapMaybe mapOne
        | _ -> Map.empty

    let insert =
        match ent.insert with
        | Ok true -> Some change
        | _ -> None

    let check =
        match ent.check with
        | Some check -> Map.singleton (check.ToFunQLString()) check
        | None -> Map.empty

    let delete =
        // Merge SELECT for all fields.
        match (selectWithKeys, ent.delete) with
        | (Some ent, Some (Ok del)) ->
            Map.fold (fun a k b -> Map.union a b) (makeRestriction del) ent |> foldRestrictionsAll
        | _ -> Map.empty

    { insert = insert
      select = select
      check = check
      update = update
      delete = delete
    }

let private flattenAllowedSchema (schema : AllowedSchema) : HalfFlatAllowedSchema =
    { entities = Map.mapMaybe (fun name -> Result.getOption >> Option.map flattenAllowedEntity) schema.entities
    }

let private flattenAllowedDatabase (db : AllowedDatabase) : HalfFlatAllowedDatabase =
    { schemas = Map.map (fun name -> flattenAllowedSchema) db.schemas
    }

type private RolesFlattener (layout : Layout, perms : Permissions) =
    let mutable flattened : Map<ResolvedRoleRef, HalfFlatAllowedDatabase> = Map.empty

    let rec halfFlattenRoleByKey (ref : ResolvedRoleRef) : HalfFlatAllowedDatabase =
        match Map.tryFind ref flattened with
        | Some res -> res
        | None ->
            let schema = Map.find ref.schema perms.schemas
            let role = Map.find ref.name schema.roles
            halfFlattenRole ref role

    and halfFlattenRole (ref : ResolvedRoleRef) (role : ResolvedRole) : HalfFlatAllowedDatabase =
            let flat = role.parents |> Set.toSeq |> Seq.map halfFlattenRoleByKey |> Seq.fold mergeAllowedDatabase (flattenAllowedDatabase role.permissions)
            flattened <- Map.add ref flat flattened
            flat

    let flattenRole (ref : ResolvedRoleRef) (role : ResolvedRole) : FlatRole =
            let flat = halfFlattenRole ref role
            { permissions = finalizeAllowedDatabase flat
            }

    let flattenPermissionsSchema schemaName (schema : PermissionsSchema) : FlatPermissionsSchema =
        let resolveOne entityName role =
            let ref = { schema = schemaName; name = entityName }
            flattenRole ref role
        { roles = Map.map resolveOne schema.roles
        }

    let flattenPermissions () : FlatPermissions =
        { schemas = Map.map flattenPermissionsSchema perms.schemas
        }

    member this.FlattenRoles = flattenPermissions

let flattenPermissions (layout : Layout) (perms : Permissions) : FlatPermissions =
    let flattener = RolesFlattener (layout, perms)
    flattener.FlattenRoles ()