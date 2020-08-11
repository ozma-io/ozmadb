module FunWithFlags.FunDB.Layout.Source

open FunWithFlags.FunUtils.Utils
open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { Columns : FieldName[]
    }

type SourceCheckConstraint =
    { Expression : string
    }

type SourceColumnField =
    { Type : string
      DefaultValue : string option
      IsNullable : bool
      IsImmutable : bool
    }

type SourceComputedField =
    { Expression : string
      AllowBroken : bool
      IsVirtual : bool
    }

type SourceField =
    | SColumnField of SourceColumnField
    | SComputedField of SourceComputedField
    | SId

type SourceEntity =
    { ColumnFields : Map<FieldName, SourceColumnField>
      ComputedFields : Map<FieldName, SourceComputedField>
      UniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints : Map<ConstraintName, SourceCheckConstraint>
      MainField : FieldName
      ForbidExternalReferences : bool
      IsHidden : bool
      IsAbstract : bool
      Parent : ResolvedEntityRef option
    } with
        member this.FindField (name : FieldName) =
            if name = funId then
                Some SId
            else if name = funMain then
                this.FindField this.MainField
            else
                match Map.tryFind name this.ColumnFields with
                | Some col -> Some <| SColumnField col
                | None ->
                    match Map.tryFind name this.ComputedFields with
                    | Some comp -> Some <| SComputedField comp
                    | None -> None

type SourceSchema =
    { Entities : Map<EntityName, SourceEntity>
    }

let emptySourceSchema : SourceSchema =
    { Entities = Map.empty
    }

let mergeSourceSchema (a : SourceSchema) (b : SourceSchema) : SourceSchema =
    { Entities = Map.unionUnique a.Entities b.Entities
    }

type SourceLayout =
    { Schemas : Map<SchemaName, SourceSchema>
    } with
        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.Entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let emptySourceLayout : SourceLayout =
    { Schemas = Map.empty }