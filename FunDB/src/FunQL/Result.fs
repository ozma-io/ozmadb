module FunWithFlags.FunDB.FunQL.Result

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.SQL.Query

// Combined data from query and the view expression.

type TypedValue =
    { valueType : AST.SimpleValueType
      value : AST.Value
    }

type TypedAttributeMap = Map<AttributeName, TypedValue>

type ResultColumn =
    { name : FunQLName
      attributes : TypedAttributeMap
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : AST.SimpleValueType
      fieldType : ResolvedFieldType option
      punType : AST.SimpleValueType option
      updateField : FieldName option
    }

type ResultViewExpr =
    { attributes : TypedAttributeMap
      rowAttributeTypes : ExecutedAttributeTypes
      columns : ResultColumn array
      rows : ExecutedRow array
      updateEntity : EntityRef option
    }

let private makeTypedAttributes = Map.map (fun name (valueType, value) -> { valueType = valueType; value = value} )

let getResultViewExpr (connection : QueryConnection) (layout : Layout) (viewExpr : ResolvedViewExpr) (arguments : Map<string, FieldValue>) : ResultViewExpr =
    let compiled = compileViewExpr layout viewExpr

    let updateEntityRef = Option.map (fun upd -> upd.entity) viewExpr.update

    let getResultColumn (attributeExprs, queryResult : ResolvedQueryResult) (column : ExecutedColumn) : ResultColumn =
        let boundField = resultBoundField queryResult
        { name = column.name
          attributes = makeTypedAttributes column.attributes
          cellAttributeTypes = column.cellAttributeTypes
          valueType = column.valueType
          fieldType =
            match boundField with
                | None -> None
                | Some ref ->
                    match Map.tryFind ref.name (layout.FindEntity(ref.entity) |> Option.get).columnFields with
                        | None -> None
                        | Some field -> Some field.fieldType
          punType = column.punType
          updateField =
            match viewExpr.update with
                | None -> None
                | Some update ->
                    match boundField with
                        | None -> None
                        | Some ref -> Map.tryFind ref.name update.fieldsToNames
        }

    runViewExpr connection compiled arguments <| fun res ->
        { attributes = makeTypedAttributes res.attributes
          rowAttributeTypes = res.rowAttributeTypes
          columns = Array.map2 getResultColumn viewExpr.results res.columns
          rows = Seq.toArray res.rows
          updateEntity = updateEntityRef
        }