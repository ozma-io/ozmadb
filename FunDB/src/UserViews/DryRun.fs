module FunWithFlags.FunDB.UserViews.DryRun

open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
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

[<NoEquality; NoComparison>]
type MainField =
    { Name : FieldName
      Field : SerializedColumnField
    }

[<NoEquality; NoComparison>]
type UVDomainField =
    { Ref : ResolvedFieldRef
      Field : SerializedColumnField option // Can be None for `id` or `sub_entity`.
      IdColumn : EntityName
    }

type UVDomain = Map<FieldName, UVDomainField>
type UVDomains = Map<GlobalDomainId, UVDomain>

[<NoEquality; NoComparison>]
type UserViewColumn =
    { Name : ViewColumnName
      AttributeTypes : ExecutedAttributeTypes
      CellAttributeTypes : ExecutedAttributeTypes
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
      MainField : MainField option
    }

[<NoEquality; NoComparison>]
type UserViewInfo =
    { AttributeTypes : ExecutedAttributeTypes
      RowAttributeTypes : ExecutedAttributeTypes
      Domains : UVDomains
      MainEntity : ResolvedEntityRef option
      Columns : UserViewColumn[]
    }

[<NoEquality; NoComparison>]
type PureAttributes =
    { Attributes : ExecutedAttributeMap
      ColumnAttributes : ExecutedAttributeMap[]
    }

[<NoEquality; NoComparison>]
type PrefetchedUserView =
    { Info : UserViewInfo
      PureAttributes : PureAttributes
      UserView : ResolvedUserView
    }

[<NoEquality; NoComparison>]
type PrefetchedViewsSchema =
    { UserViews : Map<UserViewName, Result<PrefetchedUserView, exn>>
    }

[<NoEquality; NoComparison>]
type PrefetchedUserViews =
    { Schemas : Map<SchemaName, Result<PrefetchedViewsSchema, UserViewsSchemaError>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.name schema.UserViews
            | _ -> None

let private mergePrefetchedViewsSchema (a : PrefetchedViewsSchema) (b : PrefetchedViewsSchema) =
    { UserViews = Map.unionUnique a.UserViews b.UserViews }

let mergePrefetchedUserViews (a : PrefetchedUserViews) (b : PrefetchedUserViews) =
    let mergeOne a b =
        match (a, b) with
        | (Ok schema1, Ok schema2) -> Ok (mergePrefetchedViewsSchema schema1 schema2)
        | (Error e1, Error e2) when obj.ReferenceEquals(e1, e2) -> Error e1
        | _ -> failwith "Cannot merge different error types"
    { Schemas = Map.unionWith (fun name -> mergeOne) a.Schemas b.Schemas }

type UserViewDryRunException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewDryRunException (message, null)

let private getColumn : ColumnType -> FunQLName option = function
    | CTColumn c -> Some c
    | _ -> None

let private getPureAttributes (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (res : ExecutedViewExpr) : PureAttributes =
    match compiled.attributesQuery with
    | None ->
        { Attributes = Map.empty
          ColumnAttributes = Array.map (fun _ -> Map.empty) res.ColumnAttributes
        }
    | Some attrInfo ->
        let filterPure name attrs =
            match Map.tryFind name attrInfo.pureColumnAttributes with
            | None -> Map.empty
            | Some pureAttrs -> attrs |> Map.filter (fun name _ -> Set.contains name pureAttrs)
        { Attributes = res.Attributes |> Map.filter (fun name _ -> Set.contains name attrInfo.pureAttributes)
          ColumnAttributes = Seq.map2 filterPure (Seq.mapMaybe getColumn compiled.columns) res.ColumnAttributes |> Seq.toArray
        }

let private limitOrderLimit (orderLimit : SQL.OrderLimitClause) : SQL.OrderLimitClause =
    let newLimit =
        match orderLimit.limit with
        | None -> SQL.VEValue <| SQL.VInt 0
        | Some oldLimit ->
            // FIXME: multiply by zero here
            oldLimit
    { orderLimit with limit = Some newLimit }

let rec private limitTreeView : SQL.SelectTreeExpr -> SQL.SelectTreeExpr = function
    | SQL.SSelect sel -> SQL.SSelect { sel with orderLimit = limitOrderLimit sel.orderLimit }
    | SQL.SValues values -> SQL.SValues values
    | SQL.SSetOp (op, a, b, limit) -> SQL.SSetOp (op, limitTreeView a, limitTreeView b, limit)

let rec private limitView (select : SQL.SelectExpr) : SQL.SelectExpr =
    { ctes = Map.map (fun name -> limitView) select.ctes
      tree = limitTreeView select.tree
    }

type private DryRunner (layout : Layout, conn : QueryConnection, forceAllowBroken : bool, onlyWithAllowBroken : bool option, cancellationToken : CancellationToken) =
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
        { Ref = f.ref
          Field = getSerializedField f.ref
          IdColumn = f.idColumn
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
                        Some { Name = fieldName
                               Field = getSerializedField { entity = insertInfo.entity; name = fieldName } |> Option.get
                             }

            { Name = column.Name
              AttributeTypes = column.AttributeTypes
              CellAttributeTypes = column.CellAttributeTypes
              ValueType = column.ValueType
              PunType = column.PunType
              MainField = mainField
            }

        { AttributeTypes = viewInfo.AttributeTypes
          RowAttributeTypes = viewInfo.RowAttributeTypes
          Domains = Map.map (fun id -> Map.map (fun name -> mergeDomainField)) compiled.flattenedDomains
          Columns = Seq.map2 getResultColumn (Seq.mapMaybe getColumn compiled.columns) viewInfo.Columns |> Seq.toArray
          MainEntity = Option.map (fun (main : ResolvedMainEntity) -> main.entity) viewExpr.mainEntity
        }

    let rec dryRunUserView (uv : ResolvedUserView) : Task<PrefetchedUserView> =
        task {
            let limited =
                { uv.Compiled with
                      query =
                          { uv.Compiled.query with
                                Expression = limitView uv.Compiled.query.Expression
                          }
                }
            let arguments = uv.Compiled.query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg.FieldType)

            try
                return! runViewExpr conn limited arguments cancellationToken <| fun info res ->
                    Task.FromResult
                        { UserView = uv
                          Info = mergeViewInfo uv.Resolved uv.Compiled info
                          PureAttributes = getPureAttributes uv.Resolved uv.Compiled res
                        }
            with
            | :? QueryException as err ->
                return raisefWithInner UserViewDryRunException err "Test execution error"
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (sourceSchema : SourceUserViewsSchema) (schema : UserViewsSchema) : Task<Map<UserViewName, exn> * PrefetchedViewsSchema> = task {
        let mutable errors = Map.empty

        let mapUserView (name, maybeUv : Result<ResolvedUserView, exn>) =
            task {
                let ref = { schema = schemaName; name = name }
                match maybeUv with
                | Error e -> return Some (name, Error e)
                | Ok uv when not (withThisBroken uv.AllowBroken) -> return None
                | Ok uv ->
                    try
                        let! r = dryRunUserView uv
                        return Some (name, Ok r)
                    with
                    | :? UserViewDryRunException as e ->
                        if uv.AllowBroken || forceAllowBroken then
                            if not uv.AllowBroken then
                                errors <- Map.add name (e :> exn) errors
                            return Some (name, Error (e :> exn))
                        else
                            return raisefWithInner UserViewDryRunException e "Error in user view %O" ref
            }

        let! userViews = schema.UserViews |> Map.toSeq |> Seq.mapTask mapUserView |> Task.map (Seq.catMaybes >> Map.ofSeq)
        let ret = { UserViews = userViews } : PrefetchedViewsSchema
        return (errors, ret)
    }

    let resolveUserViews (source : SourceUserViews) (resolved : UserViews) : Task<ErroredUserViews * PrefetchedUserViews> = task {
        let mutable errors : ErroredUserViews = Map.empty

        let mapSchema name = function
            | Ok schema ->
                task {
                    try
                        let! (schemaErrors, newSchema) = resolveUserViewsSchema name (Map.find name source.Schemas) schema
                        if not <| Map.isEmpty schemaErrors then
                            errors <- Map.add name (UEUserViews schemaErrors) errors
                        return Ok newSchema
                    with
                    | :? UserViewDryRunException as e ->
                        return raisefWithInner UserViewDryRunException e.InnerException "Error in schema %O: %s" name e.Message
                }
            | Error e -> Task.result (Error e)

        let! schemas = resolved.Schemas |> Map.mapTask mapSchema
        let ret = { Schemas = schemas } : PrefetchedUserViews
        return (errors, ret)
    }

    member this.DryRunAnonymousUserView uv = dryRunUserView uv
    member this.DryRunUserViews = resolveUserViews

// Warning: this should be executed outside of any transactions because of test runs.
let dryRunUserViews (conn : QueryConnection) (layout : Layout) (forceAllowBroken : bool) (onlyWithAllowBroken : bool option) (sourceViews : SourceUserViews) (userViews : UserViews) (cancellationToken : CancellationToken) : Task<ErroredUserViews * PrefetchedUserViews> =
    task {
        let runner = DryRunner(layout, conn, forceAllowBroken, onlyWithAllowBroken, cancellationToken)
        let! (errors, ret) = runner.DryRunUserViews sourceViews userViews
        return (errors, ret)
    }

let dryRunAnonymousUserView (conn : QueryConnection) (layout : Layout) (q: ResolvedUserView) (cancellationToken : CancellationToken) : Task<PrefetchedUserView> =
    let runner = DryRunner(layout, conn, false, None, cancellationToken)
    runner.DryRunAnonymousUserView q