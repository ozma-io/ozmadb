module FunWithFlags.FunDB.FunQL.Query

open System
open System.Threading.Tasks
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type ExecutedAttributeMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypes = Map<AttributeName, SQL.SimpleValueType>

type [<JsonConverter(typeof<ExecutedValueConverter>)>] [<NoComparison>] ExecutedValue =
    { attributes : ExecutedAttributeMap
      value : SQL.Value
      pun : SQL.Value option
    }
// Implemented by hand to make output smaller when there are no attributes.
and ExecutedValueConverter () =
    inherit JsonConverter<ExecutedValue> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType, existingValue, hasExistingValue, serializer : JsonSerializer) : ExecutedValue =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, res : ExecutedValue, serializer : JsonSerializer) : unit =
        let vals1 = Map.singleton "value" (res.value :> obj)
        let vals2 =
            if not <| Map.isEmpty res.attributes then
                Map.add "attributes" (res.attributes :> obj) vals1
            else
                vals1
        let vals3 =
            match res.pun with
            | None -> vals2
            | Some p -> Map.add "pun" (p :> obj) vals2
        serializer.Serialize(writer, vals3)

type [<JsonConverter(typeof<ExecutedRowConverter>)>] [<NoComparison>] ExecutedRow =
    { attributes : ExecutedAttributeMap
      values : ExecutedValue array
      entityIds : Map<EntityName, int>
      mainId : int option
      domainId : GlobalDomainId
    }
and ExecutedRowConverter () =
    inherit JsonConverter<ExecutedRow> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : ExecutedRow =
        raise <| NotImplementedException ()

    override this.WriteJson (writer : JsonWriter, res : ExecutedRow, serializer : JsonSerializer) : unit =
        // TODO: rewrite this to mutable dictionary or use "ignore nulls" policy.
        let vals1 = [("values", res.values :> obj); ("domainId", res.domainId :> obj)] |> Map.ofSeq
        let vals2 =
            if not <| Map.isEmpty res.attributes then
                Map.add "attributes" (res.attributes :> obj) vals1
            else
                vals1
        let vals3 =
            if not <| Map.isEmpty res.entityIds then
                Map.add "entityIds" (res.entityIds :> obj) vals2
            else
                vals2
        let vals4 =
            match res.mainId with
            | None -> vals3
            | Some id -> Map.add "mainId" (id :> obj) vals3
        serializer.Serialize(writer, vals4)

type ExecutedColumnInfo =
    { name : FunQLName
      attributeTypes : ExecutedAttributeTypes
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : SQL.SimpleValueType
      punType : SQL.SimpleValueType option
    }

type ExecutedViewInfo =
    { attributeTypes : ExecutedAttributeTypes
      rowAttributeTypes : ExecutedAttributeTypes
      columns : ExecutedColumnInfo array
    }

[<NoComparison>]
type ExecutedViewExpr =
    { attributes : ExecutedAttributeMap
      columnAttributes : ExecutedAttributeMap array
      rows : ExecutedRow seq
    }

[<NoComparison>]
type private ExecutedAttributes =
    { attributes : ExecutedAttributeMap
      attributeTypes : ExecutedAttributeTypes
      columnAttributes : Map<FieldName, ExecutedAttributeTypes * ExecutedAttributeMap>
    }

let private parseAttributesResult (columns : ColumnType[]) (result : QueryResult) : ExecutedAttributes =
    let values = Seq.exactlyOne result.rows

    let takeViewAttribute colType (_, valType) v =
        match colType with
        | CTRowAttribute name -> Some (name, (valType, v))
        | _ -> None
    let viewAttributes = Seq.map3Maybe takeViewAttribute columns result.columns values |> Map.ofSeq

    let takeColumnAttribute colType (_, valType) v =
        match colType with
        | CTCellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, v)))
        | _ -> None
    let makeColumnAttributes (fieldName : FieldName, values : (FieldName * (AttributeName * (SQL.SimpleValueType * SQL.Value))) seq) =
        let valuesMap = values |> Seq.map snd |> Map.ofSeq
        let attrs = Map.map (fun name -> snd) valuesMap
        let attrTypes = Map.map (fun name -> fst) valuesMap
        (fieldName, (attrTypes, attrs))
    let colAttributes =
        Seq.map3Maybe takeColumnAttribute columns result.columns values
        |> Seq.groupBy fst
        |> Seq.map makeColumnAttributes
        |> Map.ofSeq

    { attributes = Map.map (fun name -> snd) viewAttributes
      attributeTypes = Map.map (fun name -> fst) viewAttributes
      columnAttributes = colAttributes
    }

let private parseResult (domains : Domains) (columns : ColumnType[]) (result : QueryResult) : ExecutedViewInfo * ExecutedRow seq =
    let takeRowAttribute i colType (_, valType) =
        match colType with
        | CTRowAttribute name -> Some (name, (valType, i))
        | _ -> None
    let rowAttributes = Seq.mapi2Maybe takeRowAttribute columns result.columns |> Map.ofSeq

    let takeCellAttribute i colType (_, valType) =
        match colType with
            | CTCellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let allCellAttributes =
        Seq.mapi2Maybe takeCellAttribute columns result.columns
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i colType (_, valType) =
        match colType with
        | CTPunAttribute name -> Some (name, (valType, i))
        | _ -> None
    let punAttributes = Seq.mapi2Maybe takePunAttribute columns result.columns |> Map.ofSeq

    let takeDomainColumn i colType =
        match colType with
        | CTDomainColumn ns -> Some (ns, i)
        | _ -> None
    let domainColumns = Seq.mapiMaybe takeDomainColumn columns |> Map.ofSeq

    let takeIdColumn i colType =
        match colType with
        | CTIdColumn entity -> Some (entity, i)
        | _ -> None
    let idColumns = Seq.mapiMaybe takeIdColumn columns |> Map.ofSeq

    let takeMainIdColumn i colType =
        match colType with
        | CTMainIdColumn -> Some i
        | _ -> None
    let mainIdColumn = Seq.mapiMaybe takeMainIdColumn columns |> Seq.first

    let takeColumn i colType (_, valType) =
        match colType with
            | CTColumn name ->
                let cellAttributes =
                    match Map.tryFind name allCellAttributes with
                        | None -> Map.empty
                        | Some a -> a
                let punType = Option.map (fun (valType, i) -> valType) <| Map.tryFind name punAttributes
                let columnInfo =
                    { name = name
                      attributeTypes = Map.empty
                      cellAttributeTypes = Map.map (fun name (valType, i) -> valType) cellAttributes
                      valueType = valType
                      punType = punType
                    }
                Some (cellAttributes, i, columnInfo)
            | _ -> None
    let columnsMeta = Seq.mapi2Maybe takeColumn columns result.columns |> Seq.toArray

    let parseRow (values : SQL.Value array) =
        let getCell (cellAttributes, i, column) =
            let attrs = Map.map (fun name (valType, i) -> values.[i]) cellAttributes
            let pun = Option.map (fun (valType, i) -> values.[i]) <| Map.tryFind column.name punAttributes
            let value = values.[i]
            { attributes = attrs
              value = value
              pun = pun
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
            | _ -> failwith "Entity main id not found"

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta
        let domainIds = Map.mapMaybe (fun ns i -> getDomainId i) domainColumns
        let entityIds = Map.mapMaybe (fun name i -> getId i) idColumns
        let mainId = Option.map getMainId mainIdColumn

        let rec getGlobalDomainId = function
            | DSingle (id, _) -> id
            | DMulti (ns, subdoms) ->
                let localId = Map.findOrFailWith (fun () -> sprintf "No domain namespace value: %i" ns) ns domainIds
                let doms = Map.findOrFailWith (fun () -> sprintf "Unknown local domain id for namespace %i: %i" ns localId) localId subdoms
                getGlobalDomainId doms

        { attributes = rowAttrs
          values = values
          domainId = getGlobalDomainId domains
          entityIds = entityIds
          mainId = mainId
        }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta
    let rows = Seq.map parseRow result.rows

    let viewInfo =
        { columns = columns
          attributeTypes = Map.empty
          rowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
        }
    (viewInfo, rows)

// XXX: Add user access rights enforcing there later.
let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : ArgumentValues) (resultFunc : ExecutedViewInfo -> ExecutedViewExpr -> Task<'a>) : Task<'a> =
    task {
        let parameters = prepareArguments viewExpr.query.arguments arguments

        let! attrsResult = task {
            match viewExpr.attributesQuery with
            | Some attributesExpr ->
                return! connection.ExecuteQuery attributesExpr.query parameters (parseAttributesResult attributesExpr.columns >> Task.FromResult)
            | None ->
                return
                    { attributes = Map.empty
                      attributeTypes = Map.empty
                      columnAttributes = Map.empty
                    }
        }

        let getColumnInfo (col : ExecutedColumnInfo) =
            let attributeTypes =
                match Map.tryFind col.name attrsResult.columnAttributes with
                | None -> Map.empty
                | Some (attrTypes, attrs) -> attrTypes
            { col with attributeTypes = attributeTypes }

        let getColumnAttributes (col : ExecutedColumnInfo) =
            match Map.tryFind col.name attrsResult.columnAttributes with
            | None -> Map.empty
            | Some (attrTypes, attrs) -> attrs

        return! connection.ExecuteQuery (viewExpr.query.expression.ToSQLString()) parameters <| fun rawResult ->
            let (info, rows) = parseResult viewExpr.domains viewExpr.columns rawResult

            let mergedInfo =
                { info with
                      attributeTypes = attrsResult.attributeTypes
                      columns = Array.map getColumnInfo info.columns
                }
            let result =
                { attributes = attrsResult.attributes
                  columnAttributes = Array.map getColumnAttributes info.columns
                  rows = rows
                }
            resultFunc mergedInfo result
    }
