module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Utils

type LocalFieldExpr = FieldExpr<FieldName>

type PureFieldExpr = FieldExpr<Void>

type ReferenceRef =
    | RThis of FieldName
    | RRef of FieldName
    with
        override this.ToString () = this.ToFunQLName ()

        interface IFunQLName with
            member this.ToFunQLName () =
                match this with
                    | RThis name -> sprintf "%s.%s" (renderSqlName "this") (name.ToFunQLName())
                    | RRef name -> sprintf "%s.%s" (renderSqlName "ref") (name.ToFunQLName())

type ResolvedFieldType = FieldExpr<ReferenceRef>

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
    { isAbstract : bool
      // Ancestors sequence, starting with the immediate ancestor
      ancestors : EntityName array
      // Immediate descendants
      descendants : Set<EntityName>
      // Descendants closure
      descendantsClosure : Set<EntityName>
      // All non-abstract descendants, including the entity itself
      possibleSubEntities : Set<EntityName>
      columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, ResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, ResolvedCheckConstraint>
    } with
        member this.FindField (name : FieldName) =
            match Map.tryFind name columnFields with
                Some col -> Some (RColumnField col)
                None ->
                    match Map.tryFind name computedFields with
                        Some comp -> Some <| RComputedField comp
                        None -> None

type ResolvedSchema =
    { entities : Map<EntityName, ResolvedEntity>
    }

type Layout =
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
