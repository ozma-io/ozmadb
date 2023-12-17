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
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Info
open FunWithFlags.FunDB.Triggers.Merge
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.SQL.Chunk
open FunWithFlags.FunDB.SQL.Query
open FunWithFlags.FunDB.Objects.Types
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

[<NoEquality; NoComparison>]
type BoundAttributeInfo =
    { Type : SQL.SimpleValueType
      Mapping : BoundMapping option
      Const : bool
    }

[<NoEquality; NoComparison>]
type CellAttributeInfo =
    { Type : SQL.SimpleValueType
      Mapping : BoundMapping option
    }

type BoundAttributesInfoMap = Map<AttributeName, BoundAttributeInfo>
type CellAttributesInfoMap = Map<AttributeName, CellAttributeInfo>

[<NoEquality; NoComparison>]
type ViewAttributeInfo =
    { Type : SQL.SimpleValueType
      Pure : bool
    }

[<NoEquality; NoComparison>]
type RowAttributeInfo =
    { Type : SQL.SimpleValueType
    }

type ViewAttributesInfoMap = Map<AttributeName, ViewAttributeInfo>
type RowAttributesInfoMap = Map<AttributeName, RowAttributeInfo>

[<NoEquality; NoComparison>]
type UserViewColumn =
    { Name : ViewColumnName
      AttributeTypes : BoundAttributesInfoMap
      CellAttributeTypes : CellAttributesInfoMap
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
      MainField : MainField option
    }

[<NoEquality; NoComparison>]
type ArgumentInfo =
    { ArgType : ResolvedFieldType
      Optional : bool
      DefaultValue : FieldValue option
      AttributeTypes : BoundAttributesInfoMap
    }

[<NoEquality; NoComparison>]
type SerializedArgumentInfo =
    { Name : ArgumentName
      ArgType : ResolvedFieldType
      Optional : bool
      DefaultValue : FieldValue option
      AttributeTypes : BoundAttributesInfoMap
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
type MainEntityInfo =
    { Entity : ResolvedEntityRef
      ForInsert : bool
    }

[<NoEquality; NoComparison>]
type UserViewInfo =
    { AttributeTypes : ViewAttributesInfoMap
      RowAttributeTypes : RowAttributesInfoMap
      [<JsonConverter(typeof<ArgumentsPrettyConverter>)>]
      Arguments : OrderedMap<ArgumentName, ArgumentInfo>
      Domains : UVDomains
      MainEntity : MainEntityInfo option
      Columns : UserViewColumn[]
      Hash : string
    }

[<NoEquality; NoComparison>]
type ConstAttributes =
    { Attributes : ExecutedAttributesMap
      ColumnAttributes : ExecutedAttributesMap[]
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
    }

[<NoEquality; NoComparison>]
type PrefetchedUserView =
    { Info : UserViewInfo
      ConstAttributes : ConstAttributes
      UserView : ResolvedUserView
    }

[<NoEquality; NoComparison>]
type PrefetchedViewsSchema =
    { UserViews : Map<UserViewName, PossiblyBroken<PrefetchedUserView>>
    }

[<NoEquality; NoComparison>]
type PrefetchedUserViews =
    { Schemas : Map<SchemaName, PossiblyBroken<PrefetchedViewsSchema>>
    } with
        member this.Find (ref : ResolvedUserViewRef) =
            match Map.tryFind ref.Schema this.Schemas with
            | Some (Ok schema) -> Map.tryFind ref.Name schema.UserViews
            | _ -> None

let emptyPrefetchedUserViews : PrefetchedUserViews =
    { Schemas = Map.empty
    }

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
    { Schemas = Map.unionWithKey mergeOne a.Schemas b.Schemas }

type UserViewDryRunException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        UserViewDryRunException (message, innerException, isUserException innerException)

    new (message : string) = UserViewDryRunException (message, null, true)

let private getColumn : (ColumnType * SQL.ColumnName) -> FunQLName option = function
    | (CTColumn c, _) -> Some c
    | _ -> None

let private getConstAttributes (info : UserViewInfo) (res : ExecutingViewExpr) : ConstAttributes =
    let pureAttrs = res.Attributes |> Map.filter (fun name _ -> info.AttributeTypes.[name].Pure)

    let filterConstColumn (colInfo : UserViewColumn) attrs =
        attrs |> Map.filter (fun attrName _ -> colInfo.AttributeTypes.[attrName].Const)
    let pureColumnAttrs = Array.map2 filterConstColumn info.Columns res.ColumnAttributes

    let filterConstArgument (argName : ArgumentName) attrs =
        let argInfo = info.Arguments.[argName]
        attrs |> Map.filter (fun attrName _ -> argInfo.AttributeTypes.[attrName].Const)
    let pureArgumentAttrs = res.ArgumentAttributes |> Map.map filterConstArgument

    { Attributes = pureAttrs
      ColumnAttributes = pureColumnAttrs
      ArgumentAttributes = pureArgumentAttrs
    }

let private emptyLimit =
    { Offset = None
      Limit = Some (SQL.VEValue (SQL.VInt 0))
      Where = None
    }

type private DryRunMainEntity =
    { Entity : ResolvedEntityRef
      Meta : MainEntityMeta
    }

let private addColumnTypes (info : UserViewInfo) (compiled : CompiledViewExpr) : CompiledViewExpr =
    let getColumn name = info.Columns |> Array.find (fun c -> c.Name = name)
    let addType (column : CompiledColumnInfo) =
        let info =
            match column.Type with
            | CTColumn name ->
                let newColumn = getColumn name
                { column.Info with ValueType = Some newColumn.ValueType }
            | CTColumnMeta (name, metaType) ->
                let newColumn = getColumn name
                match metaType with
                | CCCellAttribute attrName ->
                    let attr = newColumn.CellAttributeTypes.[attrName]
                    { column.Info with ValueType = Some newColumn.ValueType }
                | CCPun ->
                    { column.Info with ValueType = newColumn.PunType }
            | CTMeta (CMRowAttribute attrName) ->
                let attr = info.RowAttributeTypes.[attrName]
                { column.Info with ValueType = Some attr.Type }
            | CTMeta (CMArgAttribute (argName, attrName)) ->
                let arg = info.Arguments.[argName]
                let attr = arg.AttributeTypes.[attrName]
                { column.Info with ValueType = Some attr.Type }
            | _ -> column.Info
        { column with Info = info }
    { compiled with Columns = Array.map addType compiled.Columns }

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
        let getMainEntity (main : ResolvedMainEntity) =
            let ref = getResolvedEntityRef main.Entity
            let meta = ObjectMap.findType<MainEntityMeta> main.Extra
            { Entity = ref
              Meta = meta
            }
        let mainEntity = Option.map getMainEntity viewExpr.MainEntity

        let getConstAttr (column : CompiledColumnInfo, expr : SQL.ValueExpr) =
            match column.Type with
            | CTMeta (CMRowAttribute attrName) -> Some attrName
            | _ -> None
        let constAttrsNames = compiled.SingleRowQuery.ConstColumns |> Seq.mapMaybe getConstAttr |> Set.ofSeq

        let getConstColunmAttr (column : CompiledColumnInfo, expr : SQL.ValueExpr) =
            match column.Type with
            | CTColumnMeta (colName, CCCellAttribute attrName) -> Some (colName, attrName)
            | _ -> None
        let constColumnAttrNames = compiled.SingleRowQuery.ConstColumns |> Seq.mapMaybe getConstColunmAttr |> Set.ofSeq

        let getConstArgumentAttr (column : CompiledColumnInfo, expr : SQL.ValueExpr) =
            match column.Type with
            | CTMeta (CMArgAttribute (argName, attrName)) -> Some (argName, attrName)
            | _ -> None
        let constArgumentAttrNames = compiled.SingleRowQuery.ConstColumns |> Seq.mapMaybe getConstArgumentAttr |> Set.ofSeq

        let allSingleRowColumnsInfo =
            Seq.concat
                [ Seq.map fst compiled.SingleRowQuery.ConstColumns
                  Seq.map fst compiled.SingleRowQuery.SingleRowColumns
                ]

        let getColumnAttributeMapping (column : CompiledColumnInfo) =
            match column.Type with
            | CTColumnMeta (colName, CCCellAttribute attrName) -> Option.map (Map.singleton attrName >> Map.singleton colName) column.Info.Mapping
            | _ -> None
        let columnAttributeMappings = Seq.mapMaybe getColumnAttributeMapping allSingleRowColumnsInfo |> Seq.fold (Map.unionWith Map.unionUnique) Map.empty
        let cellAttributeMappings = Seq.mapMaybe getColumnAttributeMapping compiled.Columns |> Seq.fold (Map.unionWith Map.unionUnique) Map.empty

        let getArgumentAttributeMapping (column : CompiledColumnInfo) =
            match column.Type with
            | CTMeta (CMArgAttribute (argName, attrName)) -> Option.map (Map.singleton attrName >> Map.singleton argName) column.Info.Mapping
            | _ -> None
        let argumentAttributeMappings = Seq.mapMaybe getArgumentAttributeMapping allSingleRowColumnsInfo |> Seq.fold (Map.unionWith Map.unionUnique) Map.empty

        let getResultColumn (column : ExecutedColumnInfo) : UserViewColumn =
            let mainField =
                match mainEntity with
                | None -> None
                | Some main ->
                    match Map.tryFind column.Name main.Meta.ColumnsToFields with
                    | None -> None
                    | Some fieldName ->
                        Some { Name = fieldName
                               Field = getSerializedField { Entity = main.Entity; Name = fieldName } |> Option.get
                             }

            let columnMappings = Map.findWithDefault column.Name Map.empty columnAttributeMappings
            let getColumnAttributeInfo attrName typ =
                let isConst = Set.contains (column.Name, attrName) constColumnAttrNames
                let mapping =
                    if isConst then
                        None
                    else
                        Map.tryFind attrName columnMappings
                { Type = typ
                  Const = isConst
                  Mapping = mapping
                }
            let columnAttributeTypes = Map.map getColumnAttributeInfo column.AttributeTypes

            let cellMappings = Map.findWithDefault column.Name Map.empty cellAttributeMappings
            let getCellAttributeInfo attrName typ =
                { Type = typ
                  Mapping = Map.tryFind attrName cellMappings
                }
            let cellAttributeTypes = Map.map getCellAttributeInfo column.CellAttributeTypes

            { Name = column.Name
              AttributeTypes = columnAttributeTypes
              CellAttributeTypes = cellAttributeTypes
              ValueType = column.ValueType
              PunType = column.PunType
              MainField = mainField
            }

        let columns = Array.map getResultColumn viewInfo.Columns

        let attributesStr =
            Seq.append compiled.SingleRowQuery.ConstColumns compiled.SingleRowQuery.SingleRowColumns
            |> Seq.map (fun (info, expr) -> SQL.SCExpr (Some info.Name, expr) |> string) |> String.concat ", "
        let queryStr = compiled.Query.Expression.ToString()
        let hash = String.concatWithWhitespaces [attributesStr; queryStr] |> Hash.sha1OfString |> String.hexBytes

        let getArg arg (info : ResolvedArgument) =
            match arg with
            | PLocal name ->
                let argAttrTypes = Map.tryFind name viewInfo.ArgumentAttributeTypes |> Option.defaultValue Map.empty

                let argMappings = Map.findWithDefault name Map.empty argumentAttributeMappings
                let getAttributeInfo attrName typ =
                    let isConst =Set.contains (name, attrName) constArgumentAttrNames
                    let mapping =
                        if isConst then
                            None
                        else
                            Map.tryFind attrName argMappings
                    { Type = typ
                      Const = isConst
                      Mapping = mapping
                    }
                let attributeTypes = Map.map getAttributeInfo argAttrTypes

                let argInfo =
                    { ArgType = info.ArgType
                      Optional = info.Optional
                      DefaultValue = info.DefaultValue
                      AttributeTypes = attributeTypes
                    } : ArgumentInfo
                Some (name, argInfo)
            | PGlobal name -> None
        let arguments = viewExpr.Arguments |> OrderedMap.mapWithKeysMaybe getArg

        let getViewAttributeInfo attrName typ =
            { Type = typ
              Pure = Set.contains attrName constAttrsNames
            }

        let getRowAttributeInfo attrName typ =
            { Type = typ
            }

        let getMainEntityInfo (main : ResolvedMainEntity) =
            let mainInfo = Option.get mainEntity
            { Entity = mainInfo.Entity
              ForInsert = main.ForInsert
            }

        { AttributeTypes = Map.map getViewAttributeInfo viewInfo.AttributeTypes
          RowAttributeTypes = Map.map getRowAttributeInfo viewInfo.RowAttributeTypes
          Arguments = arguments
          Domains = Map.map (fun id -> Map.map (fun name -> mergeDomainField)) compiled.FlattenedDomains
          Columns = columns
          MainEntity = Option.map getMainEntityInfo viewExpr.MainEntity
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
                      SingleRowQuery = uv.Compiled.SingleRowQuery
                }
            let arguments = uv.Compiled.Query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg)

            try
                return! runViewExpr conn layout limited comment arguments cancellationToken <| fun info res ->
                    let mergedInfo = mergeViewInfo uv.Resolved uv.Compiled info res
                    let nonpureCompiled =
                        { uv.Compiled with
                              SingleRowQuery = { uv.Compiled.SingleRowQuery with ConstColumns = [||] }
                        }
                    let nonpureUv = { uv with Compiled = addColumnTypes mergedInfo nonpureCompiled }
                    Task.FromResult
                        { UserView = nonpureUv
                          Info = mergedInfo
                          ConstAttributes = getConstAttributes mergedInfo res
                        }
            with
            | :? UserViewExecutionException as err ->
                return raisefWithInner UserViewDryRunException err "Test execution error"
        }

    let resolveUserViewsSchema (schemaName : SchemaName) (schema : UserViewsSchema) : Task<PrefetchedViewsSchema> =
        task {
            let mapUserView (name, maybeUv : PossiblyBroken<ResolvedUserView>) =
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
                                return Some (name, Error { Error = e; AllowBroken = uv.AllowBroken })
                    with
                    | e -> return raisefWithInner UserViewDryRunException e "In user view %O" name
                }

            let! userViews = schema.UserViews |> Map.toSeq |> Seq.mapTask mapUserView |> Task.map (Seq.catMaybes >> Map.ofSeq)
            return { UserViews = userViews }
        }

    let resolveUserViews (resolved : UserViews) : Task<PrefetchedUserViews> =
        task {
            let mapSchema name = function
                | Ok schema ->
                    task {
                        try
                            let! ret = resolveUserViewsSchema name schema
                            return Ok ret
                        with
                        | e -> return raisefWithInner UserViewDryRunException e "In schema %O" name
                    }
                | Error e -> Task.result (Error e)

            let! schemas = resolved.Schemas |> Map.mapTask mapSchema
            return { Schemas = schemas }
        }

    member this.DryRunAnonymousUserView uv = dryRunUserView uv
    member this.DryRunUserViews = resolveUserViews

// Warning: this should be executed outside of any transactions because of test runs.
let dryRunUserViews (conn : QueryConnection) (layout : Layout) (triggers : MergedTriggers) (forceAllowBroken : bool) (onlyWithAllowBroken : bool option) (userViews : UserViews) (cancellationToken : CancellationToken) : Task<PrefetchedUserViews> =
    task {
        let runner = DryRunner(layout, triggers, conn, forceAllowBroken, onlyWithAllowBroken, cancellationToken)
        return! runner.DryRunUserViews userViews
    }

let dryRunAnonymousUserView (conn : QueryConnection) (layout : Layout) (triggers : MergedTriggers) (q: ResolvedUserView) (cancellationToken : CancellationToken) : Task<PrefetchedUserView> =
    let runner = DryRunner(layout, triggers, conn, false, None, cancellationToken)
    runner.DryRunAnonymousUserView q None
