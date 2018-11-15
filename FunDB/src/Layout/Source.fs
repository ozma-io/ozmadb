module FunWithFlags.FunDB.Layout.Source

open Newtonsoft.Json

open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { [<JsonProperty(Required=Required.Always)>]
      columns : FunQLName array
    }

type SourceCheckConstraint =
    { [<JsonProperty(Required=Required.Always)>]
      expression : string
    }

type SourceColumnField =
    { [<JsonProperty(Required=Required.Always)>]
      fieldType : string
      defaultValue : string option
      isNullable : bool
    }

type SourceComputedField =
    { [<JsonProperty(Required=Required.Always)>]
      expression : string
    }

type SourceField =
    | SColumnField of SourceColumnField
    | SComputedField of SourceComputedField

type SourceEntity =
    { columnFields : Map<FieldName, SourceColumnField>
      computedFields : Map<FieldName, SourceComputedField>
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      [<JsonProperty(Required=Required.Always)>]
      mainField : FieldName
    } with
        member this.FindField (name : FieldName) =
            match Map.tryFind name this.columnFields with
                | Some col -> Some <| SColumnField col
                | None ->
                    match Map.tryFind name this.computedFields with
                        | Some comp -> Some <| SComputedField comp
                        | None -> None

type SourceSchema =
    { entities : Map<EntityName, SourceEntity>
    }

type SourceLayout =
    { schemas : Map<SchemaName, SourceSchema>
    } with

        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))
