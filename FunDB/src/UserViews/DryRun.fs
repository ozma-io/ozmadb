module FunWithFlags.FunDB.UserViews.DryRun

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

[<NoComparison>]
type MainField =
    { name : FieldName
      field : SerializedColumnField
    }

[<NoComparison>]
type UVDomainField =
    { ref : ResolvedFieldRef
      field : SerializedColumnField option
      idColumn : EntityName
    }

type UVDomain = Map<FieldName, UVDomainField>
type UVDomains = Map<GlobalDomainId, UVDomain>

[<NoComparison>]
type UserViewColumn =
    { name : ViewColumnName
      attributeTypes : ExecutedAttributeTypes
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : SQL.SimpleValueType
      punType : SQL.SimpleValueType option
      mainField : MainField option
    }

[<NoComparison>]
type UserViewInfo =
    { attributeTypes : ExecutedAttributeTypes
      rowAttributeTypes : ExecutedAttributeTypes
      domains : UVDomains
      mainEntity : ResolvedEntityRef option
      columns : UserViewColumn[]
    }

[<NoComparison>]
type PureAttributes =
    { attributes : ExecutedAttributeMap
      columnAttributes : ExecutedAttributeMap[]
    }

[<NoComparison>]
type PrefetchedUserView =
    { info : UserViewInfo
      pureAttributes : PureAttributes
      uv : ResolvedUserView
    }

[<NoComparison>]
type PrefetchedViewsSchema =
    { userViews : Map<UserViewName, Result<PrefetchedUserView, UserViewError>>
    }

[<NoComparison>]
type PrefetchedUserViews =
    { schemas : Map<SchemaName, PrefetchedViewsSchema>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.userViews

let private mergePrefetchedViewsSchema (a : PrefetchedViewsSchema) (b : PrefetchedViewsSchema) =
    { userViews = Map.unionUnique a.userViews b.userViews }

let mergePrefetchedUserViews (a : PrefetchedUserViews) (b : PrefetchedUserViews) =
    { schemas = Map.unionWith (fun name -> mergePrefetchedViewsSchema) a.schemas b.schemas }

type UserViewDryRunException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewDryRunException (message, null)

let private getColumn : ColumnType -> FunQLName option = function
    | CTColumn c -> Some c
    | _ -> None

let private getPureAttributes (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (res : ExecutedViewExpr) : PureAttributes =
    match compiled.attributesQuery with
    | None ->
        { attributes = Map.empty
          columnAttributes = Array.map (fun _ -> Map.empty) res.columnAttributes
        }
    | Some attrInfo ->
        let filterPure name attrs =
            match Map.tryFind name attrInfo.pureColumnAttributes with
            | None -> Map.empty
            | Some pureAttrs -> attrs |> Map.filter (fun name _ -> Set.contains name pureAttrs)
        { attributes = res.attributes |> Map.filter (fun name _ -> Set.contains name attrInfo.pureAttributes)
          columnAttributes = Seq.map2 filterPure (Seq.mapMaybe getColumn compiled.columns) res.columnAttributes |> Seq.toArray
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

type private DryRunner (layout : Layout, conn : QueryConnection, forceAllowBroken : bool, onlyWithAllowBroken : bool option) =
    let mutable serializedFields : Map<ResolvedFieldRef, SerializedColumnField> = Map.empty

    let getSerializedField (ref : ResolvedFieldRef) =
        match Map.tryFind ref serializedFields with
        | Some f -> Some f
        | None ->
            match layout.FindField ref.entity ref.name with
            | Some (_, RColumnField src) ->
                let res = serializeColumnField src
                serializedFields <- Map.add ref res serializedFields
                Some res
            | _ -> None

    let mergeDomainField (f : DomainField) : UVDomainField =
        { ref = f.ref
          field = getSerializedField f.ref
          idColumn = f.idColumn
        }

    let withThisBroken (allowBroken : bool) =
        match onlyWithAllowBroken with
        | None -> true
        | Some b -> b = allowBroken

    let mergeViewInfo (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (viewInfo : ExecutedViewInfo) : UserViewInfo =
        let mainEntity = Option.map (fun (main : ResolvedMainEntity) -> (layout.FindEntity main.entity |> Option.get, main)) viewExpr.mainEntity
        let getResultColumn name (column : ExecutedColumnInfo) : UserViewColumn =
            let mainField =
                match mainEntity with
                | None -> None
                | Some (entity, insertInfo) ->
                    match Map.tryFind name insertInfo.columnsToFields with
                    | None -> None
                    | Some fieldName ->
                        Some { name = fieldName
                               field = getSerializedField { entity = insertInfo.entity; name = fieldName } |> Option.get
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
          columns = Seq.map2 getResultColumn (Seq.mapMaybe getColumn compiled.columns) viewInfo.columns |> Seq.toArray
          mainEntity = Option.map (fun (main : ResolvedMainEntity) -> main.entity) viewExpr.mainEntity
        }

    let rec dryRunUserView (uv : ResolvedUserView) : Task<PrefetchedUserView> =
        task {
            let limited =
                { uv.compiled with
                      query =
                          { uv.compiled.query with
                                expression = limitView uv.compiled.query.expression
                          }
                }
            let arguments = uv.compiled.query.arguments.types |> Map.map (fun name arg -> defaultCompiledArgument arg.fieldType)

            try
                return! runViewExpr conn limited arguments <| fun info res ->
                    Task.FromResult
                        { uv = uv
                          info = mergeViewInfo uv.resolved uv.compiled info
                          pureAttributes = getPureAttributes uv.resolved uv.compiled res
                        }
            with
            | :? QueryException as err ->
                return raisefWithInner UserViewDryRunException err "Test execution error"
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (sourceSchema : SourceUserViewsSchema) (schema : UserViewsSchema) : Task<ErroredUserViewsSchema * PrefetchedViewsSchema> = task {
        let mutable errors = Map.empty

        let mapUserView (name, maybeUv : Result<ResolvedUserView, UserViewError>) =
            task {
                let ref = { schema = schemaName; name = name }
                match maybeUv with
                | Error e when not (withThisBroken e.source.allowBroken) -> return None
                | Error e -> return Some (name, Error e)
                | Ok uv when not (withThisBroken uv.allowBroken) -> return None
                | Ok uv ->
                    try
                        let! r = dryRunUserView uv
                        return Some (name, Ok r)
                    with
                    | :? UserViewDryRunException as e ->
                        if uv.allowBroken || forceAllowBroken then
                            if not uv.allowBroken then
                                errors <- Map.add name (e :> exn) errors
                            let err =
                                { error = e :> exn
                                  source = Map.find name sourceSchema.userViews
                                }
                            return Some (name, Error err)
                        else
                            return raisefWithInner UserViewDryRunException e "Error in user view %O" ref
            }

        let! userViews = schema.userViews |> Map.toSeq |> Seq.mapTaskSync mapUserView |> Task.map (Seq.catMaybes >> Map.ofSeq)
        let ret = { userViews = userViews } : PrefetchedViewsSchema
        return (errors, ret)
    }

    let resolveUserViews (source : SourceUserViews) (resolved : UserViews) : Task<ErroredUserViews * PrefetchedUserViews> = task {
        let mutable errors = Map.empty

        let mapSchema name schema =
            task {
                try
                    let! (schemaErrors, newSchema) = resolveUserViewsSchema name (Map.find name source.schemas) schema
                    if not <| Map.isEmpty schemaErrors then
                        errors <- Map.add name schemaErrors errors
                    return newSchema
                with
                | :? UserViewDryRunException as e ->
                    return raisefWithInner UserViewDryRunException e.InnerException "Error in schema %O: %s" name e.Message
            }

        let! schemas = resolved.schemas |> Map.mapTaskSync mapSchema
        let ret = { schemas = schemas } : PrefetchedUserViews
        return (errors, ret)
    }

    member this.DryRunAnonymousUserView uv = dryRunUserView uv
    member this.DryRunUserViews = resolveUserViews

// Warning: this should be executed outside of any transactions because of test runs.
let dryRunUserViews (conn : QueryConnection) (layout : Layout) (forceAllowBroken : bool) (onlyWithAllowBroken : bool option) (sourceViews : SourceUserViews) (userViews : UserViews) : Task<ErroredUserViews * PrefetchedUserViews> =
    task {
        let runner = DryRunner(layout, conn, forceAllowBroken, onlyWithAllowBroken)
        let! (errors, ret) = runner.DryRunUserViews sourceViews userViews
        return (errors, ret)
    }

let dryRunAnonymousUserView (conn : QueryConnection) (layout : Layout) (q: ResolvedUserView) : Task<PrefetchedUserView> =
    let runner = DryRunner(layout, conn, false, None)
    runner.DryRunAnonymousUserView q