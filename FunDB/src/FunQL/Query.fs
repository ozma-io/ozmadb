module FunWithFlags.FunDB.FunQL.Query

open System
open Newtonsoft.Json

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.SQL.Query

type CombinedAttributeMap = Map<AttributeName, AST.SimpleValueType * AST.Value>
type ExecutedAttributeMap = Map<AttributeName, AST.Value>
type ExecutedAttributeTypes = Map<AttributeName, AST.SimpleValueType>

exception ViewExecutionError of info : string with
    override this.Message = this.info

type ExecutedColumn =
    { name : FunQLName
      attributes : CombinedAttributeMap
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : AST.SimpleValueType
      punType : AST.SimpleValueType option
    }

type [<JsonConverter(typeof<ExecutedValueConverter>)>] ExecutedValue =
    { attributes : ExecutedAttributeMap
      value : AST.Value
      pun : AST.Value option
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

[<NoComparison>]
type ExecutedViewExpr =
    { attributes : CombinedAttributeMap
      rowAttributeTypes : ExecutedAttributeTypes
      columns : ExecutedColumn array
      rows : ExecutedRow seq
    }

let private typecheckArgument (fieldType : ParsedFieldType) (value : FieldValue) : unit =
    match fieldType with
        | FTEnum vals ->
            match value with
                | FString str when Set.contains str vals -> ()
                | _ -> raise (ViewExecutionError <| sprintf "Argument is not from allowed values of a enum: %O" value)
        // Most casting/typechecking will be done by database or Npgsql
        | _ -> ()

// Differs from compileFieldValue in that it doesn't emit value expressions.
let private compileArgument : FieldValue -> AST.Value = function
    | FInt i -> AST.VInt i
    // PostgreSQL cannot deduce text's type on its own
    | FString s -> AST.VString s
    | FBool b -> AST.VBool b
    | FDateTime dt -> AST.VDateTime dt
    | FDate d -> AST.VDate d
    | FIntArray vals -> AST.VIntArray (compileArray vals)
    | FStringArray vals -> AST.VStringArray (compileArray vals)
    | FBoolArray vals -> AST.VBoolArray (compileArray vals)
    | FDateTimeArray vals -> AST.VDateTimeArray (compileArray vals)
    | FDateArray vals -> AST.VDateArray (compileArray vals)
    | FNull -> AST.VNull

type private ColumnType =
    | RowAttribute of AttributeName
    | CellAttribute of FieldName * AttributeName
    | PunAttribute of FieldName
    | Column of FunQLName
    | MainEntityId

let private parseColumnName (name : AST.SQLName) =
    match name.ToString().Split("__") with
        | [|""; "Row"; name|] -> RowAttribute (FunQLName name)
        | [|""; "Cell"; columnName; name|] -> CellAttribute (FunQLName columnName, FunQLName name)
        | [|""; "Name"; name|] -> PunAttribute (FunQLName name)
        | [|""; "Id"|] -> MainEntityId
        | [|name|] -> Column (FunQLName name)
        | _ -> raise (ViewExecutionError <| sprintf "Cannot parse column name: %O" name)

let private parseAttributesResult (result : QueryResult) : CombinedAttributeMap * Map<FieldName, CombinedAttributeMap> =
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

    (viewAttributes, colAttributes)

let private parseResult (result : QueryResult) : ExecutedViewExpr =
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
                let column =
                    { name = name
                      attributes = Map.empty
                      cellAttributeTypes = Map.map (fun name (valType, i) -> valType) cellAttributes
                      valueType = valType
                      punType = punType
                    }
                Some (cellAttributes, i, column)
            | _ -> None
    let columnsMeta = allColumns |> Seq.mapiMaybe takeColumn |> Seq.toArray

    let parseRow (values : AST.Value array) =
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
                | AST.VInt id -> id
                | _ -> failwith "Main entity id is not an integer"

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta

        { attributes = rowAttrs
          values = values
          entityId = Option.map getEntityId entityIdPos
        }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta
    let rows = Seq.map parseRow result.rows

    { attributes = Map.empty
      rowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
      columns = columns
      rows = rows
    }

// XXX: Add user access rights enforcing there later.
let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : Map<string, FieldValue>) (resultFunc : ExecutedViewExpr -> 'a) : 'a =
    let makeParameter (name : string, mapping : CompiledArgument) =
        let value =
            match Map.tryFind name arguments with
                | None -> raise (ViewExecutionError <| sprintf "Argument not found: %s" name)
                | Some value -> value
        typecheckArgument mapping.fieldType value
        (mapping.placeholder, (mapping.valueType, compileArgument value))
        
    let parameters = viewExpr.arguments |> Map.toSeq |> Seq.map makeParameter |> Map.ofSeq
    let (viewAttrs, colAttrs) = connection.ExecuteQuery viewExpr.attributesStr parameters parseAttributesResult
    connection.ExecuteQuery (viewExpr.query.ToString()) parameters <| fun rawResult ->
        let result = parseResult rawResult
        let getColumn (col : ExecutedColumn) : ExecutedColumn =
            let attributes =
                match Map.tryFind col.name colAttrs with
                    | None -> Map.empty
                    | Some attrs -> attrs
            { col with attributes = attributes }
        resultFunc { attributes = viewAttrs
                     rowAttributeTypes = result.rowAttributeTypes
                     columns = Array.map getColumn result.columns
                     rows = result.rows
                   }