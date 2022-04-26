module FunWithFlags.FunDB.FunQL.Query

open System.Linq
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open FSharpPlus
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Exception
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Misc

type UserViewExecutionException (message : string, innerException : Exception, isUserException : bool) =
    inherit UserException(message, innerException, isUserException)

    new (message : string, innerException : Exception) =
        UserViewExecutionException (message, innerException, isUserException innerException)

    new (message : string) = UserViewExecutionException (message, null, true)

type ExecutedAttributesMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypesMap = Map<AttributeName, SQL.SimpleValueType>

type [<NoEquality; NoComparison>] ExecutedValue =
    { Value : SQL.Value
      [<DataMember(EmitDefaultValue = false, Order = 42)>]
      Attributes : ExecutedAttributesMap
      [<DataMember(EmitDefaultValue = false)>]
      Pun : SQL.Value option
    }

type [<NoEquality; NoComparison>] ExecutedEntityId =
    { Id : int
      [<DataMember(EmitDefaultValue = false)>]
      SubEntity : ResolvedEntityRef option
    }

type [<NoEquality; NoComparison>] ExecutedRow =
    { Values : ExecutedValue[]
      DomainId : GlobalDomainId option
      [<DataMember(EmitDefaultValue = false)>]
      Attributes : ExecutedAttributesMap
      [<DataMember(EmitDefaultValue = false)>]
      EntityIds : Map<DomainIdColumn, ExecutedEntityId>
      [<DataMember(EmitDefaultValue = false)>]
      MainId : int option
      [<DataMember(EmitDefaultValue = false)>]
      MainSubEntity : ResolvedEntityRef option
    }

[<NoEquality; NoComparison>]
type ExecutedColumnInfo =
    { Name : FunQLName
      AttributeTypes : ExecutedAttributeTypesMap
      CellAttributeTypes : ExecutedAttributeTypesMap
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
    }

[<NoEquality; NoComparison>]
type ExecutedViewInfo =
    { AttributeTypes : ExecutedAttributeTypesMap
      RowAttributeTypes : ExecutedAttributeTypesMap
      Columns : ExecutedColumnInfo[]
      ArgumentAttributeTypes : Map<ArgumentName, ExecutedAttributeTypesMap>
    }

[<NoEquality; NoComparison>]
type ExecutingViewExpr =
    { Attributes : ExecutedAttributesMap
      ColumnAttributes : ExecutedAttributesMap[]
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributesMap>
      Rows : IAsyncEnumerable<ExecutedRow>
    }

[<NoEquality; NoComparison>]
type private ExecutedAttributes =
    { Attributes : ExecutedAttributesMap
      AttributeTypes : ExecutedAttributeTypesMap
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributeTypesMap * ExecutedAttributesMap>
      ColumnAttributes : Map<FieldName, ExecutedAttributeTypesMap * ExecutedAttributesMap>
    }

// TODO: we could improve performance with pipelining (we could also pipeline attributes query).
let setPragmas (connection : QueryConnection) (pragmas : CompiledPragmasMap) (cancellationToken : CancellationToken) : Task =
    unitTask {
        for KeyValue(name, v) in pragmas do
            let q =
                { Parameter = name
                  Scope = Some SQL.SSLocal
                  Value = SQL.SVValue [|v|]
                } : SQL.SetExpr
            let! _ = connection.ExecuteNonQuery (q.ToSQLString()) Map.empty cancellationToken
            ()
    }

let unsetPragmas (connection : QueryConnection) (pragmas : CompiledPragmasMap) (cancellationToken : CancellationToken) : Task =
    unitTask {
        for KeyValue(name, v) in pragmas do
            let q =
                { Parameter = name
                  Scope = Some SQL.SSLocal
                  Value = SQL.SVDefault
                } : SQL.SetExpr
            let! _ = connection.ExecuteNonQuery (q.ToSQLString()) Map.empty cancellationToken
            ()
    }

let private splitPairsMap pairsMap =
    let fsts = Map.map (fun name -> fst) pairsMap
    let snds = Map.map (fun name -> snd) pairsMap
    (fsts, snds)

let private getAttributesQuery (viewExpr : CompiledViewExpr) : (ColumnType[] * SQL.SelectExpr) option =
    let allColumns =  Seq.append viewExpr.AttributesQuery.PureColumns viewExpr.AttributesQuery.PureColumnsWithArguments
    let colTypes = allColumns |> Seq.map (fun (typ, name, col) -> typ) |> Seq.toArray
    if Array.isEmpty colTypes then
        None
    else
        let query =
            { SQL.emptySingleSelectExpr with
                  Columns = allColumns |> Seq.map (fun (typ, name, col) -> SQL.SCExpr (Some name, col)) |> Seq.toArray
            }
        let select = SQL.selectExpr (SQL.SSelect query)
        Some (colTypes, select)

let private parseAttributesResult (columns : ColumnType[]) (values : (SQL.SQLName * SQL.SimpleValueType * SQL.Value)[]) : ExecutedAttributes =
    let takeViewAttribute colType (_, valType, v) =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, v))
        | _ -> None
    let viewAttributes = Seq.map2Maybe takeViewAttribute columns values |> Map.ofSeq

    let takeColumnAttribute colType (_, valType, v) =
        match colType with
        | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, Map.singleton name (valType, v))
        | _ -> None

    let colAttributes =
        Seq.map2Maybe takeColumnAttribute columns values
        |> Map.ofSeqWith (fun name -> Map.union)
        |> Map.map (fun fieldName -> splitPairsMap)

    let takeArgAttribute colType (_, valType, v) =
        match colType with
        | CTMeta (CMArgAttribute (argName, name)) -> Some (argName, Map.singleton name (valType, v))
        | _ -> None
    let argAttributes =
        Seq.map2Maybe takeArgAttribute columns values
        |> Map.ofSeqWith (fun name -> Map.union)
        |> Map.map (fun fieldName -> splitPairsMap)

    { Attributes = Map.map (fun name -> snd) viewAttributes
      AttributeTypes = Map.map (fun name -> fst) viewAttributes
      ColumnAttributes = colAttributes
      ArgumentAttributes = argAttributes
    }

let private parseResult
        (mainRootEntity : ResolvedEntityRef option)
        (domains : Domains)
        (columns : (ColumnType * SQL.ColumnName)[])
        (resultColumns : (SQL.SQLName * SQL.SimpleValueType)[])
        (rows : IAsyncEnumerable<SQL.Value[]>)
        (processFunc : ExecutedViewInfo -> IAsyncEnumerable<ExecutedRow> -> Task<'a>) : Task<'a> =
    let takeRowAttribute i (colType, _) (_, valType) =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, i))
        | _ -> None
    let rowAttributes = Seq.mapi2Maybe takeRowAttribute columns resultColumns |> Map.ofSeq

    let takeCellAttribute i (colType, _) (_, valType) =
        match colType with
            | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let allCellAttributes =
        Seq.mapi2Maybe takeCellAttribute columns resultColumns
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i (colType, _) (_, valType) =
        match colType with
        | CTColumnMeta (name, CCPun) -> Some (name, (valType, i))
        | _ -> None
    let punAttributes = Seq.mapi2Maybe takePunAttribute columns resultColumns |> Map.ofSeq

    let takeDomainColumn i (colType, _) =
        match colType with
        | CTMeta (CMDomain ns) -> Some (ns, i)
        | _ -> None
    let domainColumns = Seq.mapiMaybe takeDomainColumn columns |> Map.ofSeq

    let takeIdColumn i (colType, _) =
        match colType with
        | CTMeta (CMId name) -> Some (name, i)
        | _ -> None
    let idColumns = Seq.mapiMaybe takeIdColumn columns |> Map.ofSeq

    let takeSubEntityColumn i (colType, _) =
        match colType with
        | CTMeta (CMSubEntity name) -> Some (name, i)
        | _ -> None
    let subEntityColumns = Seq.mapiMaybe takeSubEntityColumn columns |> Map.ofSeq

    let takeMainIdColumn i (colType, _) =
        match colType with
        | CTMeta CMMainId -> Some i
        | _ -> None
    let mainIdColumn = Seq.mapiMaybe takeMainIdColumn columns |> Seq.first

    let takeMainSubEntityColumn i (colType, _) =
        match colType with
        | CTMeta CMMainSubEntity -> Some i
        | _ -> None
    let mainSubEntityColumn = Seq.mapiMaybe takeMainSubEntityColumn columns |> Seq.first

    let takeColumn i (colType, _) (_, valType) =
        match colType with
            | CTColumn name ->
                let cellAttributes =
                    match Map.tryFind name allCellAttributes with
                        | None -> Map.empty
                        | Some a -> a
                let punType = Option.map (fun (valType, i) -> valType) <| Map.tryFind name punAttributes
                let columnInfo =
                    { Name = name
                      AttributeTypes = Map.empty
                      CellAttributeTypes = Map.map (fun name (valType, i) -> valType) cellAttributes
                      ValueType = valType
                      PunType = punType
                    }
                Some (cellAttributes, i, columnInfo)
            | m_ -> None
    let columnsMeta = Seq.mapi2Maybe takeColumn columns resultColumns |> Seq.toArray

    let parseRow (values : SQL.Value[]) =
        let getCell (cellAttributes, i, column) =
            let attrs = Map.map (fun name (valType, i) -> values.[i]) cellAttributes
            let pun = Map.tryFind column.Name punAttributes |> Option.map (fun (valType, i) -> values.[i])
            let value = values.[i]
            { Attributes = attrs
              Value = value
              Pun = pun
            }

        let getDomainId i =
            match values.[i] with
                | SQL.VInt id -> Some id
                | SQL.VNull -> None
                | _ -> failwith "Domain id is not an integer"

        let getId i =
            match values.[i] with
                | SQL.VInt id -> Some id
                | SQL.VNull -> None
                | _ -> failwith "Entity id is not an integer"

        let getMainId i =
            match values.[i] with
            | SQL.VInt id -> id
            | _ -> failwith "Entity main id is not an integer"

        let getMainSubEntity i =
            match values.[i] with
            | SQL.VString subEntityString ->
                parseTypeName (Option.get mainRootEntity) subEntityString
            | _ -> failwith "Main subentity is not a string"

        let domainIds = Map.mapMaybe (fun ns i -> getDomainId i) domainColumns

        let rec getGlobalDomainId = function
            | DSingle (id, info) -> Some (id, info)
            | DMulti (ns, subdoms) ->
                match Map.tryFind ns domainIds with
                | None -> None
                | Some localId ->
                    let doms = Map.findOrFailWith (fun () -> sprintf "Unknown local domain id for namespace %i: %i" ns localId) localId subdoms
                    getGlobalDomainId doms

        let domainId = getGlobalDomainId domains

        let getSubEntity entity i =
            match values.[i] with
                | SQL.VString subEntityString ->
                    Some <| parseTypeName entity subEntityString
                | SQL.VNull -> None
                | _ -> failwith "Entity subtype is not a string"

        let getEntityId info =
            let mid = idColumns |> Map.find info.IdColumn |> getId
            match mid with
            // ID can be NULL, for example, in case of JOINs.
            | None -> None
            | Some id ->
                let subEntity = Option.bind (getSubEntity info.RootEntity) (Map.tryFind info.IdColumn subEntityColumns)
                let ret =
                    { Id = id
                      SubEntity = subEntity
                    }
                Some (info.IdColumn, ret)

        let getEntityIds fields =
            fields |> Map.values |> Seq.mapMaybe getEntityId |> Map.ofSeq

        let entityIds = Option.map (snd >> getEntityIds) domainId |> Option.defaultValue Map.empty

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta
        let mainId = Option.map getMainId mainIdColumn
        let mainSubEntity = Option.map getMainSubEntity mainSubEntityColumn

        { Attributes = rowAttrs
          Values = values
          DomainId = Option.map fst domainId
          EntityIds = entityIds
          MainId = mainId
          MainSubEntity = mainSubEntity
        }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta

    let info : ExecutedViewInfo =
        { Columns = columns
          AttributeTypes = Map.empty
          RowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
          ArgumentAttributeTypes = Map.empty
        }

    let rows' = rows.Select(parseRow)
    processFunc info rows'

let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (comments : string option) (arguments : ArgumentValuesMap) (cancellationToken : CancellationToken) (processFunc : ExecutedViewInfo -> ExecutingViewExpr -> Task<'a>) : Task<'a> =
    task {
        try
            let parameters = prepareArguments viewExpr.Query.Arguments arguments
            let prefix = convertComments comments

            do! setPragmas connection viewExpr.Pragmas cancellationToken

            let! attrsResult =
                task {
                    match getAttributesQuery viewExpr with
                    | None ->
                        return
                            { Attributes = Map.empty
                              AttributeTypes = Map.empty
                              ColumnAttributes = Map.empty
                              ArgumentAttributes = Map.empty
                            }
                    | Some (colTypes, query) ->
                        match! connection.ExecuteRowValuesQuery (prefix + string query) parameters cancellationToken with
                        | None -> return failwith "Unexpected empty query result"
                        | Some row -> return parseAttributesResult colTypes row
                }

            let getColumnInfo (col : ExecutedColumnInfo) =
                let attributeTypes =
                    match Map.tryFind col.Name attrsResult.ColumnAttributes with
                    | None -> Map.empty
                    | Some (attrTypes, attrs) -> attrTypes
                { col with AttributeTypes = attributeTypes }

            let getColumnAttributes (col : ExecutedColumnInfo) =
                match Map.tryFind col.Name attrsResult.ColumnAttributes with
                | None -> Map.empty
                | Some (attrTypes, attrs) -> attrs

            let! ret = connection.ExecuteQuery (prefix + viewExpr.Query.Expression.ToSQLString()) parameters cancellationToken <| fun resultColumns rawRows ->
                parseResult viewExpr.MainRootEntity viewExpr.Domains viewExpr.Columns resultColumns rawRows <| fun info rows ->
                    let mergedInfo =
                        { info with
                                ArgumentAttributeTypes = Map.map (fun name -> fst) attrsResult.ArgumentAttributes
                                AttributeTypes = attrsResult.AttributeTypes
                                Columns = Array.map getColumnInfo info.Columns
                        }
                    let result =
                        { Attributes = attrsResult.Attributes
                          ColumnAttributes = Array.map getColumnAttributes info.Columns
                          ArgumentAttributes = Map.map (fun name -> snd) attrsResult.ArgumentAttributes
                          Rows = rows
                        }
                    processFunc mergedInfo result

            do! unsetPragmas connection viewExpr.Pragmas cancellationToken
            return ret
        with
        | :? QueryException as e -> return raisefUserWithInner UserViewExecutionException e ""
        | :? ArgumentCheckException as e -> return raisefUserWithInner UserViewExecutionException e "Arguments preparation failed"
    }

type ExplainedQuery =
    { Query : string
      Parameters : ExprParameters
      Explanation : JToken
    }

type ExplainedViewExpr =
    { Rows : ExplainedQuery
      Attributes : ExplainedQuery option
    }

let explainViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (maybeArguments : ArgumentValuesMap option) (explainOpts : ExplainOptions) (cancellationToken : CancellationToken) : Task<ExplainedViewExpr> =
    task {
        try
            let arguments = Option.defaultWith (fun () -> viewExpr.Query.Arguments.Types |> Map.map (fun name arg -> defaultCompiledArgument arg)) maybeArguments
            let parameters = prepareArguments viewExpr.Query.Arguments arguments

            do! setPragmas connection viewExpr.Pragmas cancellationToken

            let! attrsResult =
                task {
                    match getAttributesQuery viewExpr with
                    | None -> return None
                    | Some (colTypes, query) ->
                        let! ret = runExplainQuery connection query parameters explainOpts cancellationToken
                        return Some { Query = string query; Parameters = parameters; Explanation = ret }
                }

            let! result = runExplainQuery connection viewExpr.Query.Expression parameters explainOpts cancellationToken

            do! unsetPragmas connection viewExpr.Pragmas cancellationToken

            return
                { Rows = { Query = string viewExpr.Query.Expression; Parameters = parameters; Explanation = result }
                  Attributes = attrsResult
                }
        with
        | :? QueryException as e -> return raisefUserWithInner UserViewExecutionException e ""
        | :? ArgumentCheckException as e -> return raisefUserWithInner UserViewExecutionException e "Arguments preparation failed"
    }
