module FunWithFlags.FunDB.Layout.Source

open Newtonsoft.Json

open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { columns : FunQLName[]
    }

type SourceCheckConstraint =
    { expression : string
    }

type SourceColumnField =
    { fieldType : string
      defaultValue : string option
      [<JsonProperty(Required=Required.DisallowNull)>]
      isNullable : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      isImmutable : bool
    }

type SourceComputedField =
    { expression : string
    }

type SourceField =
    | SColumnField of SourceColumnField
    | SComputedField of SourceComputedField
    | SId

type SourceEntity =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      columnFields : Map<FieldName, SourceColumnField>
      [<JsonProperty(Required=Required.DisallowNull)>]
      computedFields : Map<FieldName, SourceComputedField>
      [<JsonProperty(Required=Required.DisallowNull)>]
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      [<JsonProperty(Required=Required.DisallowNull)>]
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      [<JsonProperty(Required=Required.DisallowNull)>]
      forbidExternalReferences : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      hidden : bool
    } with
        member this.FindField (name : FieldName) =
            if name = funId then
                Some SId
            else if name = funMain then
                this.FindField this.mainField
            else
                match Map.tryFind name this.columnFields with
                | Some col -> Some <| SColumnField col
                | None ->
                    match Map.tryFind name this.computedFields with
                    | Some comp -> Some <| SComputedField comp
                    | None -> None

type SourceSchema =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceEntity>
    }

let emptySourceSchema : SourceSchema =
    { entities = Map.empty }

type SourceLayout =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      schemas : Map<SchemaName, SourceSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptySourceLayout : SourceLayout =
    { schemas = Map.empty }