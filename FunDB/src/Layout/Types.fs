module FunWithFlags.FunDB.Layout.Types

open FunWithFlags.FunDB.FunQL.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

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

type ResolvedFieldType = FieldType<ResolvedEntityRef, ReferenceRef>

type ResolvedReferenceFieldExpr = FieldExpr<ResolvedEntityRef, ReferenceRef>

type ResolvedUniqueConstraint =
    { columns : FunQLName array
    }

type ResolvedCheckConstraint =
    { expression : LocalFieldExpr
    }

type ResolvedColumnField =
    { fieldType : ResolvedFieldType
      valueType : SQL.SimpleValueType
      defaultValue : FieldValue option
      isNullable : bool
    }

type ResolvedComputedField =
    { expression : LinkedLocalFieldExpr
      // Set when there's no dereferences in the expression
      isLocal : bool
    }

type ResolvedField =
    | RColumnField of ResolvedColumnField
    | RComputedField of ResolvedComputedField
    | RId

type ResolvedEntity =
    { columnFields : Map<FieldName, ResolvedColumnField>
      computedFields : Map<FieldName, ResolvedComputedField>
      uniqueConstraints : Map<ConstraintName, ResolvedUniqueConstraint>
      checkConstraints : Map<ConstraintName, ResolvedCheckConstraint>
      mainField : FieldName
    } with
        member this.FindField (name : FieldName) =
            if name = funId then
                Some (funId, RId)
            else if name = funMain then
                this.FindField this.mainField
            else
                match Map.tryFind name this.columnFields with
                | Some col -> Some (name, RColumnField col)
                | None ->
                    match Map.tryFind name this.computedFields with
                    | Some comp -> Some (name, RComputedField comp)
                    | None -> None

type ResolvedSchema =
    { entities : Map<EntityName, ResolvedEntity>
    }

type Layout =
    { schemas : Map<SchemaName, ResolvedSchema>
    } with

        member this.FindEntity (entity : ResolvedEntityRef) =
            match Map.tryFind entity.schema this.schemas with
            | None -> None
            | Some schema -> Map.tryFind entity.name schema.entities

        member this.FindField (entity : ResolvedEntityRef) (field : FieldName) =
            this.FindEntity(entity) |> Option.bind (fun entity -> entity.FindField(field))

let mapAllFields (f : FieldName -> ResolvedField -> 'a) (entity : ResolvedEntity) : Map<FieldName, 'a> =
    let columns = entity.columnFields |> Map.toSeq |> Seq.map (fun (name, col) -> (name, f name (RColumnField col)))
    let computed = entity.computedFields |> Map.toSeq |> Seq.map (fun (name, comp) -> (name, f name (RComputedField comp)))
    let id = (funId, f funId RId)
    Map.ofSeq (Seq.append (Seq.singleton id) (Seq.append columns computed))
