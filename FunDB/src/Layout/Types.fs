module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Utils

type ReferenceRef =
    | RThis of FieldName
    | RRef of FieldName
    with
        override this.ToString () = this.ToFunQLString()

        member this.ToFunQLString () =
            match this with
                | RThis name -> sprintf "%s.%s" (renderSqlName "this") (name.ToFunQLString())
                | RRef name -> sprintf "%s.%s" (renderSqlName "ref") (name.ToFunQLString())

        interface IFunQLString with
            member this.ToFunQLString() = this.ToFunQLString()

type ResolvedFieldType = FieldType<EntityRef, ReferenceRef>

type ResolvedReferenceFieldExpr = FieldExpr<ReferenceRef>

type ResolvedUniqueConstraint =
    { columns : FunQLName array
    }

type ResolvedCheckConstraint =
    { expression : LocalFieldExpr
    }

type ResolvedColumnField =
    { fieldType : ResolvedFieldType
      defaultExpr : PureFieldExpr option
      isNullable : bool
    }

type ResolvedComputedField =
    { expression : LocalFieldExpr
    }

type ResolvedField =
    | RColumnField of ResolvedColumnField
    | RComputedField of ResolvedComputedField

type ResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, ResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, ResolvedCheckConstraint>
      mainField : FieldName
    } with
        member this.FindField (name : FieldName) =
            match Map.tryFind name this.columnFields with
                | Some col -> Some <| RColumnField col
                | None ->
                    match Map.tryFind name this.computedFields with
                        | Some comp -> Some <| RComputedField comp
                        | None -> None

type ResolvedSchema =
    { entities : Map<EntityName, ResolvedEntity>
    }

type Layout =
    { schemas : Map<SchemaName, ResolvedSchema>
      systemEntities : Map<EntityName, ResolvedEntity>
    } with
        member this.FindEntity (entity : EntityRef) =
            match entity.schema with
                | None -> Map.tryFind entity.name this.systemEntities
                | Some schemaName ->
                    match Map.tryFind schemaName this.schemas with
                        | None -> None
                        | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : EntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))
