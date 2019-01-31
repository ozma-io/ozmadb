module FunWithFlags.FunDB.FunQL.Query

open System
open Newtonsoft.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type ExecutedAttributeMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypes = Map<AttributeName, SQL.SimpleValueType>

exception ViewExecutionError of info : string with
    override this.Message = this.info

type [<JsonConverter(typeof<ExecutedValueConverter>)>] ExecutedValue =
    { attributes : ExecutedAttributeMap
      value : SQL.Value
      pun : SQL.Value option
    }
// Implemented by hand to make output smaller when there are no attributes.
and ExecutedValueConverter () =
    inherit JsonConverter<ExecutedValue> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType, existingValue, hasExistingValue, serializer : JsonSerializer) : ExecutedValue =
        raise <| new NotImplementedException()
 
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

type [<JsonConverter(typeof<ExecutedRowConverter>)>] ExecutedRow =
    { attributes : ExecutedAttributeMap
      values : ExecutedValue array
      entityId : int option
    }
and ExecutedRowConverter () =
    inherit JsonConverter<ExecutedRow> ()

    override this.CanRead = false

    override this.ReadJson (reader : JsonReader, objectType : Type, existingValue, hasExistingValue, serializer : JsonSerializer) : ExecutedRow =
        raise <| new NotImplementedException()
 
    override this.WriteJson (writer : JsonWriter, res : ExecutedRow, serializer : JsonSerializer) : unit =
        let vals1 = Map.singleton "values" (res.values :> obj)
        let vals2 =
            if not <| Map.isEmpty res.attributes then
                Map.add "attributes" (res.attributes :> obj) vals1
            else
                vals1
        let vals3 =
            match res.entityId with
                | None -> vals2
                | Some id -> Map.add "id" (id :> obj) vals2
        serializer.Serialize(writer, vals3)
    
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

type private ColumnType =
    | RowAttribute of AttributeName
    | CellAttribute of FieldName * AttributeName
    | PunAttribute of FieldName
    | Column of FunQLName
    | MainEntityId

let private parseColumnName (name : SQL.SQLName) =
    match name.ToString().Split("__") with
        | [|""; "Row"; name|] -> RowAttribute (FunQLName name)
        | [|""; "Cell"; columnName; name|] -> CellAttribute (FunQLName columnName, FunQLName name)
        | [|""; "Name"; name|] -> PunAttribute (FunQLName name)
        | [|""; "Id"|] -> MainEntityId
        | [|name|] -> Column (FunQLName name)
        | _ -> raise (ViewExecutionError <| sprintf "Cannot parse column name: %O" name)

type private AttributesResult =
    { attributes : ExecutedAttributeMap
      attributeTypes : ExecutedAttributeTypes
      columnAttributes : Map<FieldName, Map<AttributeName, SQL.SimpleValueType * SQL.Value>>
    }

let private parseAttributesResult (result : QueryResult) : AttributesResult =
    let values = Seq.exactlyOne result.rows

    let allColumns = result.columns |> Array.map (fun (name, typ) -> (parseColumnName name, typ))

    let takeViewAttribute i (colType, valType) =
        match colType with
            | RowAttribute name -> Some (name, (valType, values.[i]))
            | _ -> None
    let viewAttributes = allColumns |> Seq.mapiMaybe takeViewAttribute |> Map.ofSeq

    let takeColumnAttribute i (colType, valType) =
        match colType with
            | CellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, values.[i])))
            | _ -> None
    let colAttributes =
        allColumns
        |> Seq.mapiMaybe takeColumnAttribute
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    { attributes = Map.map (fun name -> snd) viewAttributes
      attributeTypes = Map.map (fun name -> fst) viewAttributes
      columnAttributes = colAttributes
    }

let private parseResult (result : QueryResult) : (ExecutedViewInfo * ExecutedRow seq) =
    let allColumns = result.columns |> Array.map (fun (name, typ) -> (parseColumnName name, typ))

    let takeRowAttribute i (colType, valType) =
        match colType with
            | RowAttribute name -> Some (name, (valType, i))
            | _ -> None
    let rowAttributes = allColumns |> Seq.mapiMaybe takeRowAttribute |> Map.ofSeq

    let takeCellAttribute i (colType, valType) =
        match colType with
            | CellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let allCellAttributes =
        allColumns
        |> Seq.mapiMaybe takeCellAttribute
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i (colType, valType) =
        match colType with
            | PunAttribute name -> Some (name, (valType, i))
            | _ -> None
    let punAttributes = allColumns |> Seq.mapiMaybe takePunAttribute |> Map.ofSeq

    let takeMainEntityId i (colType, valType) =
        match colType with
            | MainEntityId -> Some i
            | _ -> None
    let entityIdPos = allColumns |> Seq.mapiMaybe takeMainEntityId |> Seq.first

    let takeColumn i (colType, valType) =
        match colType with
            | Column name ->
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
    let columnsMeta = allColumns |> Seq.mapiMaybe takeColumn |> Seq.toArray

    let parseRow (values : SQL.Value array) =
        let getCell (cellAttributes, i, column) =
            let attrs = Map.map (fun name (valType, i) -> values.[i]) cellAttributes
            let pun = Option.map (fun (valType, i) -> values.[i]) <| Map.tryFind column.name punAttributes
            let value = values.[i]
            { attributes = attrs
              value = value
              pun = pun
            }

        let getEntityId i =
            match values.[i] with
                | SQL.VInt id -> id
                | _ -> failwith "Main entity id is not an integer"

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta

        { attributes = rowAttrs
          values = values
          entityId = Option.map getEntityId entityIdPos
        }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta
    let rows = Seq.map parseRow result.rows

    let viewInfo =
        { columns = columns
          attributeTypes = Map.empty
          rowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
        }
    (viewInfo, rows)

type ViewArguments = Map<Placeholder, FieldValue>

// XXX: Add user access rights enforcing there later.
let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : ViewArguments) (resultFunc : ExecutedViewInfo -> ExecutedViewExpr -> 'a) : 'a =
    let makeParameter (name : Placeholder) (mapping : CompiledArgument) =
        let value =
            match Map.tryFind name arguments with
            | None -> raise (ViewExecutionError <| sprintf "Argument not found: %O" name)
            | Some value -> value
        match typecheckArgument mapping.fieldType value with
        | Ok () -> ()
        | Error msg -> raise (ViewExecutionError msg)
        (mapping.placeholder, (mapping.valueType, compileArgument value))
    let parameters = viewExpr.arguments |> Map.mapWithKeys makeParameter

    let attrsResult =
        match viewExpr.attributesExpr with
        | Some attributesExpr -> connection.ExecuteQuery attributesExpr.query parameters parseAttributesResult
        | None ->
            { attributes = Map.empty
              attributeTypes = Map.empty
              columnAttributes = Map.empty
            }

    let getColumnInfo (col : ExecutedColumnInfo) =
        let attributeTypes =
            match Map.tryFind col.name attrsResult.columnAttributes with
            | None -> Map.empty
            | Some attrs -> Map.map (fun name -> fst) attrs
        { col with attributeTypes = attributeTypes }

    let getColumnAttributes (col : ExecutedColumnInfo) =
        match Map.tryFind col.name attrsResult.columnAttributes with
        | None -> Map.empty
        | Some attrs -> Map.map (fun name -> snd) attrs

    connection.ExecuteQuery (viewExpr.query.ToString()) parameters <| fun rawResult ->
        let (info, rows) = parseResult rawResult

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