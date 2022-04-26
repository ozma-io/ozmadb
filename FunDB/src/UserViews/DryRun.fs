module FunWithFlags.FunDB.UserViews.DryRun

open System
open System.Threading
open System.Threading.Tasks
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.UserViews.Types
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.SQL.Chunk
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
      IdColumn : DomainIdColumn
    }

type UVDomain = Map<FieldName, UVDomainField>
type UVDomains = Map<GlobalDomainId, UVDomain>

type AttributeInfo =
    { Type : SQL.SimpleValueType
      Pure : bool
    }

type AttributesInfoMap = Map<AttributeName, AttributeInfo>

[<NoEquality; NoComparison>]
type UserViewColumn =
    { Name : ViewColumnName
      AttributeTypes : AttributesInfoMap
      CellAttributeTypes : ExecutedAttributeTypesMap
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
      MainField : MainField option
    }

[<NoEquality; NoComparison>]
type ArgumentInfo =
    { ArgType : ResolvedFieldType
      Optional : bool
      DefaultValue : FieldValue option
      AttributeTypes : AttributesInfoMap
    }

[<NoEquality; NoComparison>]
type SerializedArgumentInfo =
    { Name : ArgumentName
      ArgType : ResolvedFieldType
      Optional : bool
      DefaultValue : FieldValue option
      AttributeTypes : AttributesInfoMap
    }

type ArgumentsPrettyConverter () =
    inherit JsonConverter<OrderedMap<ArgumentName, ArgumentInfo>> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, someType, existingValue, hasExistingValue, serializer : JsonSerializer) : OrderedMap<ArgumentName, ArgumentInfo> =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, value : OrderedMap<ArgumentName, ArgumentInfo>, serializer : JsonSerializer) : unit =
        writer.WriteStartArray ()
        for KeyValue(k, v) in value do
            let convInfo =
                { Name = k
                  ArgType = v.ArgType
                  Optional = v.Optional
                  DefaultValue = v.DefaultValue
                  AttributeTypes = v.AttributeTypes
                }
            serializer.Serialize (writer, convInfo)
        writer.WriteEndArray ()

[<NoEquality; NoComparison>]
type UserViewInfo =
    { AttributeTypes : AttributesInfoMap
      RowAttributeTypes : ExecutedAttributeTypesMap
      [<JsonConverter(typeof<ArgumentsPrettyConverter>)>]
      Arguments : OrderedMap<ArgumentName, ArgumentInfo>
      Domains : UVDomains
      MainEntity : ResolvedEntityRef option
      Columns : UserViewColumn[]
      Hash : string
    }

[<NoEquality; NoComparison>]
type PureAttributes =
    { Attributes : ExecutedAttributesMap
      ColumnAttributes : ExecutedAttributesMap[]
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
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
            match Map.tryFind ref.Schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.Name schema.UserViews
            | _ -> None

let private mergePrefetchedViewsSchema (a : PrefetchedViewsSchema) (b : PrefetchedViewsSchema) =
    { UserViews = Map.unionUnique a.UserViews b.UserViews }

let mergePrefetchedUserViews (a : PrefetchedUserViews) (b : PrefetchedUserViews) =
    let mergeOne name a b =
        match (a, b) with
        | (Ok schema1, Ok schema2) ->
            try
                Ok (mergePrefetchedViewsSchema schema1 schema2)
            with
            | Failure e -> failwithf "Error in schema %O: %s" name e
        | _ -> failwith "Cannot merge different error types"
    { Schemas = Map.unionWith mergeOne a.Schemas b.Schemas }

type UserViewDryRunException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        UserViewDryRunException (message, innerException, isUserException innerException)

    new (message : string) = UserViewDryRunException (message, null, true)

let private getColumn : (ColumnType * SQL.ColumnName) -> FunQLName option = function
    | (CTColumn c, _) -> Some c
    | _ -> None

let private getPureAttributes (info : UserViewInfo) (res : ExecutingViewExpr) : PureAttributes =
    let pureAttrs = res.Attributes |> Map.filter (fun name _ -> info.AttributeTypes.[name].Pure)

    let filterColumnPure (colInfo : UserViewColumn) attrs =
        attrs |> Map.filter (fun attrName _ -> colInfo.AttributeTypes.[attrName].Pure)
    let pureColumnAttrs = Array.map2 filterColumnPure info.Columns res.ColumnAttributes

    let filterArgumentPure (argName : ArgumentName) attrs =
        let argInfo = info.Arguments.[argName]
        attrs |> Map.filter (fun attrName _ -> argInfo.AttributeTypes.[attrName].Pure)
    let pureArgumentAttrs = res.ArgumentAttributes |> Map.map filterArgumentPure

    { Attributes = pureAttrs
      ColumnAttributes = pureColumnAttrs
      ArgumentAttributes = pureArgumentAttrs
    }

let private emptyLimit =
    { Offset = None
      Limit = Some (SQL.VEValue (SQL.VInt 0))
      Where = None
    }

type private DryRunner (layout : Layout, triggers : MergedTriggers, conn : QueryConnection, forceAllowBroken : bool, onlyWithAllowBroken : bool option, cancellationToken : CancellationToken) =
    let mutable serializedFields : Map<ResolvedFieldRef, SerializedColumnField> = Map.empty

    let getSerializedField (ref : ResolvedFieldRef) =
        match Map.tryFind ref serializedFields with
        | Some f -> Some f
        | None ->
            match layout.FindField ref.Entity ref.Name with
            | Some { Field = RColumnField src } ->
                let triggersEntity = Option.defaultValue emptyMergedTriggersEntity (triggers.FindEntity ref.Entity)
                let res = serializeColumnField triggersEntity ref.Name src
                serializedFields <- Map.add ref res serializedFields
                Some res
            | _ -> None

    let mergeDomainField (f : DomainField) : UVDomainField =
        { Ref = f.Ref
          Field = getSerializedField f.Ref
          IdColumn = f.IdColumn
        }

    let withThisBroken (allowBroken : bool) =
        match onlyWithAllowBroken with
        | None -> true
        | Some b -> b = allowBroken

    let mergeViewInfo (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (viewInfo : ExecutedViewInfo) (results : ExecutingViewExpr) : UserViewInfo =
        let mainEntity = Option.map (fun (main : ResolvedMainEntity) -> (layout.FindEntity main.Entity |> Option.get, main)) viewExpr.MainEntity

        let getPureAttr = function
        | (CTMeta (CMRowAttribute attrName), name, attr) -> Some attrName
        | _ -> None
        let pureAttrsNames = compiled.AttributesQuery.PureColumns |> Seq.mapMaybe getPureAttr |> Set.ofSeq

        let getPureColumnAttr = function
        | (CTColumnMeta (colName, CCCellAttribute attrName), name, attr) -> Some (colName, attrName)
        | _ -> None
        let pureColumnAttrNames = compiled.AttributesQuery.PureColumns |> Seq.mapMaybe getPureColumnAttr |> Set.ofSeq

        let getPureArgumentAttr = function
        | (CTMeta (CMArgAttribute (argName, attrName)), name, attr) -> Some (argName, attrName)
        | _ -> None
        let pureArgumentAttrNames = compiled.AttributesQuery.PureColumns |> Seq.mapMaybe getPureArgumentAttr |> Set.ofSeq

        let getResultColumn name (column : ExecutedColumnInfo) : UserViewColumn =
            let mainField =
                match mainEntity with
                | None -> None
                | Some (entity, insertInfo) ->
                    match Map.tryFind name insertInfo.ColumnsToFields with
                    | None -> None
                    | Some fieldName ->
                        Some { Name = fieldName
                               Field = getSerializedField { Entity = insertInfo.Entity; Name = fieldName } |> Option.get
                             }
             
            let getAttributeInfo attrName typ =
                { Type = typ
                  Pure = Set.contains (name, attrName) pureColumnAttrNames
                }

            { Name = column.Name
              AttributeTypes = Map.map getAttributeInfo column.AttributeTypes
              CellAttributeTypes = column.CellAttributeTypes
              ValueType = column.ValueType
              PunType = column.PunType
              MainField = mainField
            }

        let attributesStr =
            Seq.append compiled.AttributesQuery.PureColumns compiled.AttributesQuery.PureColumnsWithArguments
            |> Seq.map (fun (typ, name, expr) -> SQL.SCExpr (Some name, expr) |> string) |> String.concat ", "
        let queryStr = compiled.Query.Expression.ToString()
        let hash = String.concatWithWhitespaces [attributesStr; queryStr] |> Hash.sha1OfString |> String.hexBytes

        let getArg arg (info : ResolvedArgument) =
            match arg with
            | PLocal name ->
                let argAttrTypes = Map.tryFind name viewInfo.ArgumentAttributeTypes |> Option.defaultValue Map.empty

                let getAttributeInfo attrName typ =
                    { Type = typ
                      Pure = Set.contains (name, attrName) pureArgumentAttrNames
                    }

                let argInfo =
                    { ArgType = info.ArgType
                      Optional = info.Optional
                      DefaultValue = info.DefaultValue
                      AttributeTypes = Map.map getAttributeInfo argAttrTypes
                    } : ArgumentInfo
                Some (name, argInfo)
            | PGlobal name -> None
        let arguments = viewExpr.Arguments |> OrderedMap.mapWithKeysMaybe getArg

        let getAttributeInfo attrName typ =
            { Type = typ
              Pure = Set.contains attrName pureAttrsNames
            }

        { AttributeTypes = Map.map getAttributeInfo viewInfo.AttributeTypes
          RowAttributeTypes = viewInfo.RowAttributeTypes
          Arguments = arguments
          Domains = Map.map (fun id -> Map.map (fun name -> mergeDomainField)) compiled.FlattenedDomains
          Columns = Seq.map2 getResultColumn (Seq.mapMaybe getColumn compiled.Columns) viewInfo.Columns |> Seq.toArray
          MainEntity = Option.map (fun (main : ResolvedMainEntity) -> main.Entity) viewExpr.MainEntity
          Hash = hash
        }

    let rec dryRunUserView (uv : ResolvedUserView) (comment : string option) : Task<PrefetchedUserView> =
        task {
            let limited =
                { uv.Compiled with
                      Query =
                          { uv.Compiled.Query with
                                Expression = selectExprChunk emptyLimit uv.Compiled.Query.Expression
                          }
                      AttributesQuery = uv.Compiled.AttributesQuery
                }
            let arguments = uv.Compiled.Query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg)

            try
                return! runViewExpr conn limited comment arguments cancellationToken <| fun info res ->
                    let nonpureCompiled =
                        { uv.Compiled with
                              AttributesQuery = { uv.Compiled.AttributesQuery with PureColumns = [||] }
                        }
                    let nonpureUv = { uv with Compiled = nonpureCompiled }
                    let mergedInfo = mergeViewInfo uv.Resolved uv.Compiled info res
                    Task.FromResult
                        { UserView = nonpureUv
                          Info = mergedInfo
                          PureAttributes = getPureAttributes mergedInfo res
                        }
            with
            | :? UserViewExecutionException as err ->
                return raisefWithInner UserViewDryRunException err "Test execution error"
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : UserViewsSchema) : Task<Map<UserViewName, exn> * PrefetchedViewsSchema> = task {
        let mutable errors = Map.empty

        let mapUserView (name, maybeUv : Result<ResolvedUserView, exn>) =
            task {
                try
                    let ref = { Schema = schemaName; Name = name }
                    match maybeUv with
                    | Error e when not (withThisBroken true) -> return None
                    | Error e -> return Some (name, Error e)
                    | Ok uv when not (withThisBroken uv.AllowBroken) -> return None
                    | Ok uv ->
                        try
                            let comment = sprintf "Dry-run of %O" ref
                            let! r = dryRunUserView uv (Some comment)
                            return Some (name, Ok r)
                        with
                        | :? UserViewDryRunException as e when uv.AllowBroken || forceAllowBroken ->
                            if not uv.AllowBroken then
                                errors <- Map.add name (e :> exn) errors
                            return Some (name, Error (e :> exn))
                with
                | e -> return raisefWithInner UserViewDryRunException e "In user view %O" name
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
                        let! (schemaErrors, newSchema) = resolveUserViewsSchema name schema
                        if not <| Map.isEmpty schemaErrors then
                            errors <- Map.add name (UEUserViews schemaErrors) errors
                        return Ok newSchema
                    with
                    | e -> return raisefWithInner UserViewDryRunException e "In schema %O" name
                }
            | Error e -> Task.result (Error e)

        let! schemas = resolved.Schemas |> Map.mapTask mapSchema
        let ret = { Schemas = schemas } : PrefetchedUserViews
        return (errors, ret)
    }

    member this.DryRunAnonymousUserView uv = dryRunUserView uv
    member this.DryRunUserViews = resolveUserViews

// Warning: this should be executed outside of any transactions because of test runs.
let dryRunUserViews (conn : QueryConnection) (layout : Layout) (triggers : MergedTriggers) (forceAllowBroken : bool) (onlyWithAllowBroken : bool option) (sourceViews : SourceUserViews) (userViews : UserViews) (cancellationToken : CancellationToken) : Task<ErroredUserViews * PrefetchedUserViews> =
    task {
        let runner = DryRunner(layout, triggers, conn, forceAllowBroken, onlyWithAllowBroken, cancellationToken)
        let! (errors, ret) = runner.DryRunUserViews sourceViews userViews
        return (errors, ret)
    }

let dryRunAnonymousUserView (conn : QueryConnection) (layout : Layout) (triggers : MergedTriggers) (q: ResolvedUserView) (cancellationToken : CancellationToken) : Task<PrefetchedUserView> =
    let runner = DryRunner(layout, triggers, conn, false, None, cancellationToken)
    runner.DryRunAnonymousUserView q None