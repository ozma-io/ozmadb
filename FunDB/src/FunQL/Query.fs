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

type ExecutedViewExpr =
    { attributes : ExecutedAttributeMap
      columns : (FunQLName * ExecutedColumn) array
      values : (ExecutedValue array) array
    }

let private typecheckArgument (fieldType : ParsedFieldType) (value : FieldValue) : () =
    match fieldType with
        | FTEnum vals ->
            match value with
                | FString str when Set.contains str vals -> ()
                | _ -> raise <| ViewExecutionError <| sprintf "Argument is not from allowed values of a enum: %O" value
        // Most casting/typechecking will be done by database or Npgsql
        | _ _> ()

// XXX: Add user access rights enforcing there later.
let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : Map<FunQLName, FieldValue>) : ExecutedViewExpr =
    let makeParameter (name, mapping) =
        let value =
            match Map.tryFind name arguments with
                | None -> raise <| ViewExecutionError <| sprintf "Argument not found: %s" name
                | Some value -> value
        typecheckArgument
        
    let parameters = viewExpr.arguments |> Map.toSeq |> Seq.map (fun (name, value) -> 
