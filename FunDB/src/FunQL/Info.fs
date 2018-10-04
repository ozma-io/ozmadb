module FunWithFlags.FunDB.FunQL.Info

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

// Combined data from query and the view expression.

type UpdateFieldInfo =
    { name : FieldName
      field : ResolvedColumnField
    }

type MergedColumnInfo =
    { name : FunQLName
      attributeTypes : ExecutedAttributeTypes
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : SQL.SimpleValueType
      fieldType : ResolvedFieldType option
      punType : SQL.SimpleValueType option
      updateField : UpdateFieldInfo option
    }

type MergedViewInfo =
    { attributeTypes : ExecutedAttributeTypes
      rowAttributeTypes : ExecutedAttributeTypes
      updateEntity : EntityRef option
      columns : MergedColumnInfo array
    }

let mergeViewInfo (layout : Layout) (viewExpr : ResolvedViewExpr) (viewInfo : ExecutedViewInfo) : MergedViewInfo =
    let updateEntity = Option.map (fun upd -> (layout.FindEntity(upd.entity) |> Option.get, upd)) viewExpr.update

    let getResultColumn (attributeExprs, queryResult : ResolvedQueryResult) (column : ExecutedColumnInfo) : MergedColumnInfo =
        let boundField = resultBoundField queryResult
        { name = column.name
          attributeTypes = column.attributeTypes
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
            match updateEntity with
                | None -> None
                | Some (entity, updateInfo) ->
                    match boundField with
                        | None -> None
                        | Some ref ->
                            if Map.containsKey ref.name updateInfo.fieldsToNames then
                                Some { name = ref.name
                                       field = Map.find ref.name entity.columnFields
                                     }
                            else
                                None
        }

    { attributeTypes = viewInfo.attributeTypes
      rowAttributeTypes = viewInfo.rowAttributeTypes
      columns = Array.map2 getResultColumn viewExpr.results viewInfo.columns
      updateEntity = Option.map (fun upd -> upd.entity) viewExpr.update
    }