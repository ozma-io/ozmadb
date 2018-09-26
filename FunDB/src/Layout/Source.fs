module FunWithFlags.FunDB.Layout.Source

open FunWithFlags.FunDB.FunQL.AST

// Source Layout; various layout sources, like database or system layout, are converted into this.

type SourceUniqueConstraint =
    { columns : FunQLName array
    }

type SourceCheckConstraint =
    { expression : string
    }

type SourceColumnField =
    { fieldType : string
      defaultExpr : string option
      isNullable : bool
    }

type SourceComputedField =
    { expression : string
    }

type private SourceField =
    | SColumnField of SourceColumnField
    | SComputedField of SourceComputedField

type SourceEntity =
    { isAbstract : bool
      ancestor : EntityName option
      columnFields : Map<FieldName, SourceColumnField>
      computedFields : Map<FieldName, SourceComputedField>
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
    } with
        member this.FindField (name : FieldName) =
            match Map.tryFind name this.columnFields with
                Some col -> Some (SColumnField col)
                None ->
                    match Map.tryFind name this.computedFields with
                        Some comp -> Some <| SComputedField comp
                        None -> None

type SourceSchema =
    { entities : Map<EntityName, ResolvedEntity>
    }

type SourceLayout =
    { schemas : Map<SchemaName, ResolvedSchema>
      systemEntities : Map<EntityName, ResolvedEntity>
    } with
        member this.FindEntity (entity : EntityRef) =
            match entity.schema with
                | None -> Map.tryFind entity.name systemEntities
                | Some schemaName ->
                    match Map.tryFind schemaName schemas with
                        | None -> None
                        | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : EntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(name))
