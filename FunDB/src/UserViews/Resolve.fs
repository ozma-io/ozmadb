module FunWithFlags.FunDB.UserViews.Resolve

open System.Threading.Tasks
open Microsoft.EntityFrameworkCore
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.Lex
open FunWithFlags.FunDB.FunQL.Parse
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Dereference
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.Attributes.Merge
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type UserViewResolveException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewResolveException (message, null)

let private checkName (FunQLName name) =
    if not (goodName name) then
        raisef UserViewResolveException "Invalid user view name"

let rec private getColumns : ResolvedSelectExpr -> ResolvedQueryResult[] = function
    | SSelect query -> query.results
    | SSetOp (op, a, b, limits) ->
        // Columns should be the same
        getColumns a

let private getPureAttributes (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (res : ExecutedViewExpr) : PureAttributes =
    match compiled.attributesQuery with
    | None ->
        { attributes = Map.empty
          columnAttributes = Array.map (fun _ -> Map.empty) res.columnAttributes
        }
    | Some attrInfo ->
        let filterPure (result : ResolvedQueryResult) attrs =
            let name = resultName result.result
            match Map.tryFind name attrInfo.pureColumnAttributes with
            | None -> Map.empty
            | Some pureAttrs -> attrs |> Map.filter (fun name _ -> Set.contains name pureAttrs)
        { attributes = res.attributes |> Map.filter (fun name _ -> Set.contains name attrInfo.pureAttributes)
          columnAttributes = Array.map2 filterPure (getColumns viewExpr.select) res.columnAttributes
        }

let private mergeDomainField (f : DomainField) : UVDomainField =
    { ref = f.ref
      field =
          match f.field with
          | RColumnField col -> Some col
          | _ -> None
      idColumn = f.idColumn
    }

let private makeMainEntity (main : ResolvedMainEntity) : MainEntityInfo =
    { entity = main.entity
      name = main.name
    }

let private limitOrderLimit (orderLimit : SQL.OrderLimitClause) : SQL.OrderLimitClause =
    let newLimit =
        match orderLimit.limit with
        | None -> SQL.VEValue <| SQL.VInt 0
        | Some oldLimit ->
            // FIXME: multiply by zero here
            oldLimit
    { orderLimit with limit = Some newLimit }

let rec private limitView : SQL.SelectExpr -> SQL.SelectExpr = function
    | SQL.SSelect sel -> SQL.SSelect { sel with orderLimit = limitOrderLimit sel.orderLimit }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp (op, a, b, limit) -> SQL.SSetOp (op, limitView a, limitView b, limit)

[<NoComparison>]
type private HalfResolvedView =
    { source : SourceUserView
      resolved : ResolvedViewExpr
      allowBroken : bool
    }

type private HalfResolvedSchema = Map<UserViewName, Result<HalfResolvedView, UserViewError>>
type private HalfResolvedViews = Map<SchemaName, HalfResolvedSchema>

type private Phase1Resolver (layout : Layout, forceAllowBroken : bool) =
    let resolveUserView (uv : SourceUserView) : HalfResolvedView =
        let parsed =
            match parse tokenizeFunQL viewExpr uv.query with
            | Error msg -> raisef UserViewResolveException "Parse error: %s" msg
            | Ok rawExpr -> rawExpr
        let resolved =
            try
                resolveViewExpr layout parsed
            with
            | :? ViewResolveException as err -> raisefWithInner UserViewResolveException err "Resolve error"
        { source = uv
          resolved = resolved
          allowBroken = uv.allowBroken
        }

    let resolveUserViewsSchema (schema : SourceUserViewsSchema) : HalfResolvedSchema =
        let mapUserView name uv =
            try
                checkName name
                try
                    Ok <| resolveUserView uv
                with
                | :? UserViewResolveException as e ->
                    if forceAllowBroken || uv.allowBroken then
                        let err =
                            { error = e :> exn
                              source = uv
                            }
                        Error err
                    else
                        reraise ()
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e.InnerException "Error in user view %O: %s" name e.Message
        schema.userViews |> Map.map mapUserView

    let resolveUserViews (uvs : SourceUserViews) : HalfResolvedViews =
        let mapSchema name schema =
            try
                if not <| Map.containsKey name layout.schemas then
                    raisef UserViewResolveException "Unknown schema name"
                resolveUserViewsSchema schema
            with
            | :? UserViewResolveException as e -> raisefWithInner UserViewResolveException e.InnerException "Error in schema %O: %s" name e.Message
        uvs.schemas |> Map.map mapSchema

    member this.ResolveUserView = resolveUserView
    member this.ResolveUserViews = resolveUserViews

type UserViewFatalResolveException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewFatalResolveException (message, null)

type private Phase2Resolver (layout : Layout, defaultAttrs : MergedDefaultAttributes, conn : QueryConnection, initialViews : Map<SchemaName, UserViewsSchema>, halfResolved : HalfResolvedViews, forceAllowBroken : bool) =
    let mutable cachedViews : Map<ResolvedUserViewRef, Result<ResolvedUserView, UserViewError>> = Map.empty

    let findCached (ref : ResolvedUserViewRef) =
        match Map.tryFind ref cachedViews with
        | Some r -> Some r
        | None ->
            match Map.tryFind ref.schema initialViews with
            | Some schema -> Map.tryFind ref.name schema.userViews
            | None -> None

    let mergeViewInfo (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (viewInfo : ExecutedViewInfo) : UserViewInfo =
        let mainEntity = Option.map (fun (main : ResolvedMainEntity) -> (layout.FindEntity main.entity |> Option.get, main)) viewExpr.mainEntity
        let getResultColumn (result : ResolvedQueryResult) (column : ExecutedColumnInfo) : UserViewColumn =
            let name = resultName result.result
            let mainField =
                match mainEntity with
                | None -> None
                | Some (entity, insertInfo) ->
                    match Map.tryFind name insertInfo.columnsToFields with
                    | None -> None
                    | Some fieldName ->
                        let field = Map.find fieldName entity.columnFields
                        Some { name = fieldName
                               field = field
                             }

            { name = column.name
              attributeTypes = column.attributeTypes
              cellAttributeTypes = column.cellAttributeTypes
              valueType = column.valueType
              punType = column.punType
              mainField = mainField
            }

        { attributeTypes = viewInfo.attributeTypes
          rowAttributeTypes = viewInfo.rowAttributeTypes
          domains = Map.map (fun id -> Map.map (fun name -> mergeDomainField)) compiled.flattenedDomains
          columns = Array.map2 getResultColumn (getColumns viewExpr.select) viewInfo.columns
          mainEntity = Option.map makeMainEntity viewExpr.mainEntity
        }

    let rec resolveUserView (stack : Set<ResolvedUserViewRef>) (homeSchema : SchemaName option) (uv : HalfResolvedView) : Task<ResolvedUserView> =
        let checkView ref =
            task {
                let schema =
                    match Map.tryFind ref.schema halfResolved with
                    | None -> raisef UserViewResolveException "Referenced view not found: %O" ref
                    | Some r -> r
                if not <| Map.containsKey ref.name schema then
                    raisef UserViewResolveException "Referenced view not found: %O" ref
            }

        task {
            let! dereferenced = task {
                try
                    return! dereferenceViewExpr checkView homeSchema uv.resolved
                with
                | :? ViewDereferenceException as err -> return raisefWithInner UserViewResolveException err "Dereference error"
            }
            let compiled = compileViewExpr layout defaultAttrs dereferenced
            let limited =
                { compiled with
                      query =
                          { compiled.query with
                                expression = limitView compiled.query.expression
                          }
                }
            let arguments = compiled.query.arguments.types |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType)

            try
                return! runViewExpr conn limited arguments <| fun info res ->
                    Task.FromResult
                        { resolved = dereferenced
                          compiled = compiled
                          info = mergeViewInfo dereferenced compiled info
                          pureAttributes = getPureAttributes dereferenced compiled res
                          allowBroken = uv.allowBroken
                        }
            with
            | :? QueryException as err -> return raisefWithInner UserViewResolveException err "Test execution error"
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : HalfResolvedSchema) : Task<ErroredUserViewsSchema * UserViewsSchema> = task {
        let mutable errors = Map.empty

        let mapUserView name maybeUv =
            task {
                let ref = { schema = schemaName; name = name }
                match maybeUv with
                | Error e ->
                    cachedViews <- Map.add ref (Error e) cachedViews
                    if not e.source.allowBroken then
                        errors <- Map.add name e.error errors
                    return Error e
                | Ok uv ->
                    match findCached ref with
                    | Some (Error e) ->
                        if not e.source.allowBroken then
                            errors <- Map.add name e.error errors
                        return Error e
                    | Some (Ok uv) ->
                        return Ok uv
                    | None ->
                        let! r =
                            task {
                                try
                                    let! r = resolveUserView (Set.singleton ref) (Some schemaName) uv
                                    return Ok r
                                with
                                | :? UserViewResolveException as e ->
                                    if uv.allowBroken || forceAllowBroken then
                                        if not uv.source.allowBroken then
                                            errors <- Map.add name (e :> exn) errors
                                        let err =
                                            { error = e :> exn
                                              source = uv.source
                                            }
                                        return Error err
                                    else
                                        return raisefWithInner UserViewResolveException e "Error in user view %O" ref
                                | :? UserViewFatalResolveException as e ->
                                    return raisefWithInner UserViewResolveException e.InnerException "%s" e.Message
                            }
                        cachedViews <- Map.add ref r cachedViews
                        return r
            }

        let! userViews = schema |> Map.mapTaskSync mapUserView
        let ret = { userViews = userViews } : UserViewsSchema
        return (errors, ret)
    }

    let resolveUserViews () : Task<ErroredUserViews * UserViews> = task {
        let mutable errors = Map.empty

        let mapSchema name schema =
            task {
                try
                    let! (schemaErrors, newSchema) = resolveUserViewsSchema name schema
                    if not <| Map.isEmpty schemaErrors then
                        errors <- Map.add name schemaErrors errors
                    return newSchema
                with
                | :? UserViewResolveException as e ->
                    return raisefWithInner UserViewResolveException e.InnerException "Error in schema %O: %s" name e.Message
            }

        let! schemas = halfResolved |> Map.mapTaskSync mapSchema
        let ret = { schemas = schemas } : UserViews
        return (errors, ret)
    }

    member this.ResolveAnonymousUserView uv =
        assert (Map.isEmpty halfResolved)
        resolveUserView Set.empty None uv
    member this.ResolveUserViews = resolveUserViews

let resolveUserViews (conn : QueryConnection) (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (forceAllowBroken : bool) (userViews : SourceUserViews) : Task<ErroredUserViews * UserViews> =
    task {
        let phase1 = Phase1Resolver(layout, forceAllowBroken)
        let resolvedViews = phase1.ResolveUserViews userViews
        let phase2 = Phase2Resolver(layout, defaultAttrs, conn, Map.empty, resolvedViews, forceAllowBroken)
        let! (errors, ret) = phase2.ResolveUserViews ()
        return (errors, ret)
    }

let resolveAnonymousUserView (conn : QueryConnection) (layout : Layout) (defaultAttrs : MergedDefaultAttributes) (existingViews : UserViews) (q: string) : Task<ResolvedUserView> =
    let phase1 = Phase1Resolver(layout, false)
    let resolvedView = phase1.ResolveUserView { query = q; allowBroken = false }
    let phase2 = Phase2Resolver(layout, defaultAttrs, conn, existingViews.schemas, Map.empty, false)
    phase2.ResolveAnonymousUserView resolvedView