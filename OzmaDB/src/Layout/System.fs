module OzmaDB.Layout.System

open System
open System.Reflection
open System.ComponentModel.DataAnnotations
open Newtonsoft.Json.Linq

open OzmaDB.OzmaUtils
open OzmaDB.SQL.Utils
open OzmaDB.Layout.Source
open OzmaDB.OzmaQL.AST
open OzmaDBSchema.Attributes

let private makeSourceColumnField (property: PropertyInfo) : (OzmaQLName * SourceColumnField) option =
    let field = property.GetCustomAttribute<ColumnFieldAttribute>()
    // Check also that property is not inherited.
    if property.DeclaringType <> property.ReflectedType || isNull field then
        None
    else
        let name = OzmaQLName(snakeCaseName property.Name)
        let requiredAttr = property.GetCustomAttribute<RequiredAttribute>()

        let res =
            { Type = field.Type
              DefaultValue = if isNull field.Default then None else Some field.Default
              IsNullable = isNull requiredAttr && not property.PropertyType.IsPrimitive
              IsImmutable = field.IsImmutable
              Description = ""
              Metadata = JsonMap.empty }

        Some(name, res)

let private makeSourceComputedField (field: ComputedFieldAttribute) : OzmaQLName * SourceComputedField =
    let res =
        { Expression = field.Expression
          AllowBroken = false
          IsVirtual = field.IsVirtual
          IsMaterialized = field.IsMaterialized
          Description = ""
          Metadata = JsonMap.empty }

    (OzmaQLName field.Name, res)

let private makeSourceUniqueConstraint (constr: UniqueConstraintAttribute) : OzmaQLName * SourceUniqueConstraint =
    let res =
        { Columns = Array.map OzmaQLName constr.Columns
          IsAlternateKey = constr.IsAlternateKey
          Description = ""
          Metadata = JsonMap.empty }

    (OzmaQLName constr.Name, res)

let private makeSourceCheckConstraint (constr: CheckConstraintAttribute) : OzmaQLName * SourceCheckConstraint =
    let res =
        { Expression = constr.Expression
          Description = ""
          Metadata = JsonMap.empty }

    (OzmaQLName constr.Name, res)

let private makeSourceIndex (index: IndexAttribute) : OzmaQLName * SourceIndex =
    let res =
        { Expressions = index.Expressions
          IncludedExpressions =
            if isNull index.IncludedExpressions then
                [||]
            else
                index.IncludedExpressions
          IsUnique = index.IsUnique
          Predicate = Option.ofObj index.Predicate
          Type =
            index.Type
            |> Option.ofObj
            |> Option.map (fun typ -> indexTypesMap.[typ])
            |> Option.defaultValue ITBTree
          Description = ""
          Metadata = JsonMap.empty }

    (OzmaQLName index.Name, res)

let private getAttributes<'t when 't :> Attribute> (prop: MemberInfo) =
    Attribute.GetCustomAttributes(prop, typeof<'t>) |> Array.map (fun x -> x :?> 't)

let private makeSourceEntity (prop: PropertyInfo) : (OzmaQLName * Type * SourceEntity) option =
    match prop.GetCustomAttribute<EntityAttribute>() with
    | null -> None
    | entityAttr ->
        let name = OzmaQLName(snakeCaseName prop.Name)
        // Should be DbSet<Foo>
        let entityClass = prop.PropertyType.GetGenericArguments().[0]

        let columnFields =
            entityClass.GetProperties() |> Seq.mapMaybe makeSourceColumnField |> Map.ofSeq

        let computedFields = getAttributes<ComputedFieldAttribute> prop
        let uniqueConstraints = getAttributes<UniqueConstraintAttribute> prop
        let checkConstraints = getAttributes<CheckConstraintAttribute> prop
        let indexes = getAttributes<IndexAttribute> prop

        let res =
            { ColumnFields = columnFields
              ComputedFields = computedFields |> Seq.map makeSourceComputedField |> Map.ofSeq
              UniqueConstraints = uniqueConstraints |> Seq.map makeSourceUniqueConstraint |> Map.ofSeq
              CheckConstraints = checkConstraints |> Seq.map makeSourceCheckConstraint |> Map.ofSeq
              Indexes = indexes |> Seq.map makeSourceIndex |> Map.ofSeq
              MainField = Some <| OzmaQLName entityAttr.MainField
              InsertedInternally = entityAttr.InsertedInternally
              UpdatedInternally = entityAttr.UpdatedInternally
              DeletedInternally = entityAttr.DeletedInternally
              TriggersMigration = entityAttr.TriggersMigration
              SaveRestoreKey = entityAttr.SaveRestoreKey |> Option.ofObj |> Option.map OzmaQLName
              IsHidden = entityAttr.IsHidden
              IsFrozen = entityAttr.IsFrozen
              Parent = None
              IsAbstract = entityClass.IsAbstract
              Description = ""
              Metadata = JsonMap.empty }

        Some(name, entityClass, res)

// Build entities map using mish-mash of our custom attributes and Entity Framework Core declarations.
let buildSystemSchema (contextClass: Type) : SourceSchema =
    let entitiesInfo =
        contextClass.GetProperties() |> Seq.mapMaybe makeSourceEntity |> Seq.cache

    let types =
        entitiesInfo
        |> Seq.map (fun (name, propType, entity) -> (propType.FullName, name))
        |> Map.ofSeq

    let applyParent (entity: SourceEntity) (propType: Type) =
        match propType.BaseType with
        | null as baseType
        | baseType when baseType = typeof<obj> -> entity
        | baseType ->
            { entity with
                Parent =
                    Some
                        { Schema = funSchema
                          Name = Map.find baseType.FullName types } }

    let entities =
        entitiesInfo
        |> Seq.map (fun (name, propType, entity) -> (name, applyParent entity propType))
        |> Map.ofSeq

    { Entities = entities
      Description = ""
      Metadata = JsonMap.empty }
