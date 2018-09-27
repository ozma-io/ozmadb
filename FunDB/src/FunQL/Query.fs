module FunWithFlags.FunDB.FunQL.Query

open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.SQL.Query

type ExecutedAttributeMap = AttributeMap<ValueType * Value>

exception ViewExecutionError of info : string with
    override this.Message = this.info

type ExecutedColumn =
    { attributes : ExecutedAttributeMap
      valueType : AST.ValueType
    }

type ExecutedValue =
    { attributes : ExecutedAttributeMap
      value : AST.Value
    }

type ExecutedRow =
    { attributes : ExecutedAttributeMap
      values : ExecutedValue array
    }

type ExecutedViewExpr =
    { attributes : ExecutedAttributeMap
      columns : (FunQLName * ExecutedColumn) array
      rows : ExecutedRow array
    }

let private typecheckArgument (fieldType : ParsedFieldType) (value : FieldValue) : unit =
    match fieldType with
        | FTEnum vals ->
            match value with
                | FString str when Set.contains str vals -> ()
                | _ -> raise <| ViewExecutionError <| sprintf "Argument is not from allowed values of a enum: %O" value
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
    | Column of FunQLName

let private parseColumnName (name : SQLName) =
    match name.ToString().Split("__") with
        | [|"", "Row", name|] -> RowAttribute (FunQLName name)
        | [|"", "Cell", columnName, name|] -> CellAttribute (FunQLName columnName, FunQLName name)
        | [|name|] -> Column (FunQLName name)
        | _ -> raise <| ViewExecutionError <| sprintf "Cannot parse column name: %O" name

let private parseAttributesResult (result : QueryResult) : ExecutedAttributeMap * Map<FieldName, ExecutedAttributeMap> =
    assert Array.length result.rows = 1
    let values = result.rows.[0]

    let allColumns = result.columns |> Array.map (fun (name, typ) -> (parseColumnName name, typ))

    let takeViewAttribute i (colType, valType) =
        match colType with
            | RowAttribute name -> Some (name, (valType, values.[i]))
            | _ -> None
    let viewAttributes = allColumns |> seqMapiMaybe takeViewAttribute |> Map.ofSeq
    let takeColumnAttribute i (colType, valType) =
        match colType with
            | CellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, values.[i])))
            | _ -> None
    let colAttributes =
        allColumns
        |> seqMapiMaybe takeColumnAttribute
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, Map.ofSeq attrs))
        |> Map.ofSeq

    (rowAttributes, cellAttributes)

let private parseResult (result : QueryResult) : ExecutedViewExpr =
    let allColumns = result.columns |> Array.map (fun (name, typ) -> (parseColumnName name, typ))

    let takeRowAttribute i (colType, valType) =
        match colType with
            | RowAttribute name -> Some (name, (valType, i))
            | _ -> None
    let rowAttributes = allColumns |> seqMapiMaybe takeRowAttribute |> Map.ofSeq
    let takeCellAttribute i (colType, valType) =
        match colType with
            | CellAttribute (fieldName, name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let cellAttributes =
        allColumns
        |> seqMapiMaybe takeCellAttribute
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, Map.ofSeq attrs))
        |> Map.ofSeq
    let takeColumn i (colType, valType) =
        match colType with
            | Column name ->
                let attributes =
                    match Map.tryFind name cellAttributes with
                        | None -> Map.empty
                        | Some a -> a
                Some (name, attributes, valType, i)
            | _ -> None
    let columns = allColumns |> seqMapiMaybe takeColumn |> Seq.toArray

    let parseRow values =
        let getAttribute name (valType, i) =
            let value = values.[i]
            (valType, value)
        let getCell (name, attributes, valType, i) =
            let attrs = Map.map getAttribute attributes
            let value = values.[i]
            { attributes = attrs
              value = value
            }

        let rowAttrs = Map.map getAttribute rowAttributes
        let values = Array.map getCell columns
        { attributes = rowAttrs
          values = values
        }

    let rows = Array.map parseRow result.rows

    { attributes = Map.empty
      columns = columns |> Array.map (fun (name, attributes, valType, i) -> (name, { attributes = Map.empty; valueType = valType }))
      rows = rows
    }

type ExecutedColumn =
    { attributes : ExecutedAttributeMap
      valueType : AST.ValueType
    }

type ExecutedValue =
    { attributes : ExecutedAttributeMap
      value : AST.Value
    }

type ExecutedRow =
    { attributes : ExecutedAttributeMap
      values : ExecutedValue array
    }

type ExecutedViewExpr =
    { attributes : ExecutedAttributeMap
      columns : (FunQLName * ExecutedColumn) array
      rows : ExecutedRow array
    }



// XXX: Add user access rights enforcing there later.
let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : Map<FunQLName, FieldValue>) : ExecutedViewExpr =
    let makeParameter (name, mapping) =
        let value =
            match Map.tryFind name arguments with
                | None -> raise <| ViewExecutionError <| sprintf "Argument not found: %s" name
                | Some value -> value
        typecheckArgument mapping.fieldType value
        (mapping.placeholder, (mapping.valueType, compileArgument value))
        
    let parameters = viewExpr.arguments |> Map.toSeq |> makeParameter |> Map.ofSeq
    let (viewAttrs, colAttrs) = connection.ExecuteQuery viewExpr.attributesStr parameters |> parseAttributesResult
    let result = connection.ExecuteQuery (viewExpr.query.ToString()) parameters |> parseResult
    { attributes = viewAttrs
      columns = Array.map (fun (name, col) -> (name, { col with attributes = colAttrs.[name] }))
      rows = result.rows
    }
