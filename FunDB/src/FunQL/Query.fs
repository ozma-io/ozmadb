module FunWithFlags.FunDB.FunQL.Query

open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json.Linq

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.Misc

type UserViewExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewExecutionException (message, null)

type ExecutedAttributeMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypes = Map<AttributeName, SQL.SimpleValueType>

type [<NoEquality; NoComparison>] ExecutedValue =
    { Value : SQL.Value
      [<DataMember(EmitDefaultValue = false, Order = 42)>]
      Attributes : ExecutedAttributeMap
      [<DataMember(EmitDefaultValue = false)>]
      Pun : SQL.Value option
    }

type [<NoEquality; NoComparison>] ExecutedEntityId =
    { Id : int
      [<DataMember(EmitDefaultValue = false)>]
      SubEntity : ResolvedEntityRef option
    }

type [<NoEquality; NoComparison>] ExecutedRow =
    { Values : ExecutedValue array
      DomainId : GlobalDomainId option
      [<DataMember(EmitDefaultValue = false)>]
      Attributes : ExecutedAttributeMap
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
      AttributeTypes : ExecutedAttributeTypes
      CellAttributeTypes : ExecutedAttributeTypes
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
    }

[<NoEquality; NoComparison>]
type ExecutedViewInfo =
    { AttributeTypes : ExecutedAttributeTypes
      RowAttributeTypes : ExecutedAttributeTypes
      Columns : ExecutedColumnInfo array
      ArgumentAttributeTypes : Map<ArgumentName, ExecutedAttributeTypes>
    }

[<NoEquality; NoComparison>]
type ExecutedViewExpr =
    { Attributes : ExecutedAttributeMap
      ColumnAttributes : ExecutedAttributeMap array
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributeMap>
      Rows : ExecutedRow seq
    }

[<NoEquality; NoComparison>]
type private ExecutedAttributes =
    { Attributes : ExecutedAttributeMap
      AttributeTypes : ExecutedAttributeTypes
      ArgumentAttributes : Map<ArgumentName, ExecutedAttributeTypes * ExecutedAttributeMap>
      ColumnAttributes : Map<FieldName, ExecutedAttributeTypes * ExecutedAttributeMap>
    }

// TODO: we could improve performance with pipelining (we could also pipeline attributes query).
let private setPragmas (connection : QueryConnection) (pragmas : CompiledPragmasMap) (cancellationToken : CancellationToken) : Task =
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

let private unsetPragmas (connection : QueryConnection) (pragmas : CompiledPragmasMap) (cancellationToken : CancellationToken) : Task =
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

let private parseAttributesResult (columns : ColumnType[]) (result : QueryResult) : ExecutedAttributes =
    let values = Seq.exactlyOne result.Rows

    let takeViewAttribute colType (_, valType) v =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, v))
        | _ -> None
    let viewAttributes = Seq.map3Maybe takeViewAttribute columns result.Columns values |> Map.ofSeq

    let takeColumnAttribute colType (_, valType) v =
        match colType with
        | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, Map.singleton name (valType, v))
        | _ -> None

    let colAttributes =
        Seq.map3Maybe takeColumnAttribute columns result.Columns values
        |> Map.ofSeqWith (fun name -> Map.union)
        |> Map.map (fun fieldName -> splitPairsMap)

    let takeArgAttribute colType (_, valType) v =
        match colType with
        | CTMeta (CMArgAttribute (argName, name)) -> Some (argName, Map.singleton name (valType, v))
        | _ -> None
    let argAttributes =
        Seq.map3Maybe takeArgAttribute columns result.Columns values
        |> Map.ofSeqWith (fun name -> Map.union)
        |> Map.map (fun fieldName -> splitPairsMap)

    { Attributes = Map.map (fun name -> snd) viewAttributes
      AttributeTypes = Map.map (fun name -> fst) viewAttributes
      ColumnAttributes = colAttributes
      ArgumentAttributes = argAttributes
    }

let private parseResult (mainEntity : ResolvedEntityRef option) (domains : Domains) (columns : (ColumnType * SQL.ColumnName)[]) (result : QueryResult) : ExecutedViewInfo * ExecutedRow seq =
    let takeRowAttribute i (colType, _) (_, valType) =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, i))
        | _ -> None
    let rowAttributes = Seq.mapi2Maybe takeRowAttribute columns result.Columns |> Map.ofSeq

    let takeCellAttribute i (colType, _) (_, valType) =
        match colType with
            | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let allCellAttributes =
        Seq.mapi2Maybe takeCellAttribute columns result.Columns
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i (colType, _) (_, valType) =
        match colType with
        | CTColumnMeta (name, CCPun) -> Some (name, (valType, i))
        | _ -> None
    let punAttributes = Seq.mapi2Maybe takePunAttribute columns result.Columns |> Map.ofSeq

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
            | _ -> None
    let columnsMeta = Seq.mapi2Maybe takeColumn columns result.Columns |> Seq.toArray

    let parseRow (values : SQL.Value array) =
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
                parseTypeName (Option.get mainEntity) subEntityString
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
                let subEntity = Option.bind (getSubEntity info.Ref.Entity) (Map.tryFind info.IdColumn subEntityColumns)
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
    let rows = Seq.map parseRow result.Rows

    let viewInfo =
        { Columns = columns
          AttributeTypes = Map.empty
          RowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
          ArgumentAttributeTypes = Map.empty
        }
    (viewInfo, rows)

let private getAttributesQuery (viewExpr : CompiledViewExpr) : (ColumnType[] * SQL.SelectExpr) option =
    let allColumns =  Seq.append viewExpr.AttributesQuery.PureColumns viewExpr.AttributesQuery.AttributeColumns
    let colTypes = allColumns |> Seq.map (fun (typ, name, col) -> typ) |> Seq.toArray
    if Array.isEmpty colTypes then
        None
    else
        let query =
            { Columns = allColumns |> Seq.map (fun (typ, name, col) -> SQL.SCExpr (Some name, col)) |> Seq.toArray
              From = None
              Where = None
              GroupBy = [||]
              OrderLimit = SQL.emptyOrderLimitClause
              Extra = null
            } : SQL.SingleSelectExpr
        let select =
            { CTEs = None
              Tree = SQL.SSelect query
              Extra = null
            } : SQL.SelectExpr
        Some (colTypes, select)

let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (comments : string option) (arguments : ArgumentValuesMap) (cancellationToken : CancellationToken) (resultFunc : ExecutedViewInfo -> ExecutedViewExpr -> Task<'a>) : Task<'a> =
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
                        return! connection.ExecuteQuery (prefix + string query) parameters cancellationToken (parseAttributesResult colTypes >> Task.FromResult)
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

            let! ret = connection.ExecuteQuery (prefix + viewExpr.Query.Expression.ToSQLString()) parameters cancellationToken <| fun rawResult ->
                let (info, rows) = parseResult viewExpr.MainEntity viewExpr.Domains viewExpr.Columns rawResult

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
                resultFunc mergedInfo result

            do! unsetPragmas connection viewExpr.Pragmas cancellationToken
            return ret
        with
        | :? QueryException as e -> return raisefWithInner UserViewExecutionException e ""
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
        | :? QueryException as e -> return raisefWithInner UserViewExecutionException e ""
    }
