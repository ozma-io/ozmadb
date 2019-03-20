module FunWithFlags.FunDB.FunQL.Info

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.FunQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

// Combined data from query and the view expression.

type MainFieldInfo =
    { name : FieldName
      field : ResolvedColumnField
    }

type MergedColumnInfo =
    { name : FunQLName
      attributeTypes : ExecutedAttributeTypes
      cellAttributeTypes : ExecutedAttributeTypes
      valueType : SQL.SimpleValueType
      punType : SQL.SimpleValueType option
      mainField : MainFieldInfo option
    }

type MergedDomainField = {
    ref : ResolvedFieldRef
    field : ResolvedColumnField
    idColumn : EntityName
}

type MergedDomain = Map<FieldName, MergedDomainField>
type MergedDomains = Map<GlobalDomainId, MergedDomain>

type MainEntityInfo =
  { entity : ResolvedEntityRef
    name : EntityName
  }

type MergedViewInfo =
    { attributeTypes : ExecutedAttributeTypes
      rowAttributeTypes : ExecutedAttributeTypes
      domains : MergedDomains
      mainEntity : MainEntityInfo option
      columns : MergedColumnInfo[]
    }

type PureAttributes =
    { attributes : ExecutedAttributeMap
      columnAttributes : ExecutedAttributeMap[]
    }

let getPureAttributes (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (res : ExecutedViewExpr) : PureAttributes =
    match compiled.attributesQuery with
    | None ->
        { attributes = Map.empty
          columnAttributes = Array.map (fun _ -> Map.empty) res.columnAttributes
        }
    | Some attrInfo ->
        let filterPure (name : FieldName) attrs =
            match Map.tryFind name attrInfo.pureColumnAttributes with
            | None -> Map.empty
            | Some pureAttrs -> attrs |> Map.filter (fun name _ -> Set.contains name pureAttrs)
        { attributes = res.attributes |> Map.filter (fun name _ -> Set.contains name attrInfo.pureAttributes)
          columnAttributes = Array.map2 filterPure viewExpr.columns res.columnAttributes
        }

let mergeViewInfo (layout : Layout) (viewExpr : ResolvedViewExpr) (compiled : CompiledViewExpr) (viewInfo : ExecutedViewInfo) : MergedViewInfo =
    let mainEntity = Option.map (fun (main : ResolvedMainEntity) -> (layout.FindEntity main.entity |> Option.get, main)) viewExpr.mainEntity
    let getResultColumn (name : FieldName) (column : ExecutedColumnInfo) : MergedColumnInfo =
        let mainField =
            match mainEntity with
            | None -> None
            | Some (entity, insertInfo) ->
                match Map.tryFind name insertInfo.columnsToFields with
                | None -> None
                | Some fieldName ->
                    let field = Map.find fieldName entity.columnFields
                    Some { name = fieldName
                           field = field
                         }

        { name = column.name
          attributeTypes = column.attributeTypes
          cellAttributeTypes = column.cellAttributeTypes
          valueType = column.valueType
          punType = column.punType
          mainField = mainField
        }

    let mergeDomainField (f : DomainField) : MergedDomainField =
        let entity = Option.getOrFailWith (fun () -> sprintf "Cannot find an entity from domain: %O" f.field.entity) <| layout.FindEntity f.field.entity
        let field = Map.findOrFailWith (fun () -> sprintf "Cannot find a field from domain: %O" f.field) f.field.name entity.columnFields
        { ref = f.field
          field = field
          idColumn = f.idColumn
        }

    let makeMainEntity (main : ResolvedMainEntity) =
      { entity = main.entity
        name = main.name
      }

    { attributeTypes = viewInfo.attributeTypes
      rowAttributeTypes = viewInfo.rowAttributeTypes
      domains = Map.map (fun id -> Map.map (fun name -> mergeDomainField)) compiled.flattenedDomains
      columns = Array.map2 getResultColumn viewExpr.columns viewInfo.columns
      mainEntity = Option.map makeMainEntity viewExpr.mainEntity
    }