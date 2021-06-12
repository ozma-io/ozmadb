module FunWithFlags.FunDB.Layout.System

open System
open System.Reflection
open System.ComponentModel.DataAnnotations

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDBSchema.Attributes

let private makeSourceColumnField (property : PropertyInfo) : (FunQLName * SourceColumnField) option =
    let field = Attribute.GetCustomAttribute(property, typeof<ColumnFieldAttribute>) :?> ColumnFieldAttribute
    // Check also that property is not inherited.
    if property.DeclaringType <> property.ReflectedType || isNull field
    then None
    else
        let name = FunQLName (snakeCaseName property.Name)
        let requiredAttr = Attribute.GetCustomAttribute(property, typeof<RequiredAttribute>) :?> RequiredAttribute
        let res =
            { Type = field.Type
              DefaultValue =
                  if isNull field.Default
                  then None
                  else Some field.Default
              IsNullable = isNull requiredAttr && not property.PropertyType.IsPrimitive
              IsImmutable = field.IsImmutable
            }
        Some (name, res)

let private makeSourceComputedField (field : ComputedFieldAttribute) : FunQLName * SourceComputedField =
    let res =
        { Expression = field.Expression
          AllowBroken = false
          IsVirtual = field.IsVirtual
        }
    (FunQLName field.Name, res)

let private makeSourceUniqueConstraint (constr : UniqueConstraintAttribute) : FunQLName * SourceUniqueConstraint =
    let res = { Columns = Array.map FunQLName constr.Columns }
    (FunQLName constr.Name, res)

let private makeSourceCheckConstraint (constr : CheckConstraintAttribute) : FunQLName * SourceCheckConstraint =
    let res = { Expression = constr.Expression } : SourceCheckConstraint
    (FunQLName constr.Name, res)

let private makeSourceIndex (index : IndexAttribute) : FunQLName * SourceIndex =
    let res =
        { Expressions = index.Expressions
          IsUnique = index.IsUnique
          Predicate = Option.ofObj index.Predicate
          Type = index.Type |> Option.ofObj |> Option.map (fun typ -> Map.find typ indexTypesMap) |> Option.defaultValue ITBTree
        }
    (FunQLName index.Name, res)

let private getAttributes<'t when 't :> Attribute> (prop : MemberInfo) =
    Attribute.GetCustomAttributes(prop, typeof<'t>) |> Array.map (fun x -> x :?> 't)

let private makeSourceEntity (prop : PropertyInfo) : (FunQLName * Type * SourceEntity) option =
    match Attribute.GetCustomAttribute(prop, typeof<EntityAttribute>) with
    | null -> None
    | :? EntityAttribute as entityAttr ->
        let name = FunQLName (snakeCaseName prop.Name)
        // Should be DbSet<Foo>
        let entityClass = prop.PropertyType.GetGenericArguments().[0]
        let columnFields = entityClass.GetProperties() |> Seq.mapMaybe makeSourceColumnField |> Map.ofSeq
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
              MainField = FunQLName entityAttr.MainField
              ForbidExternalReferences = entityAttr.ForbidExternalReferences
              ForbidTriggers = entityAttr.ForbidTriggers
              TriggersMigration = entityAttr.TriggersMigration
              IsHidden = entityAttr.IsHidden
              IsFrozen = entityAttr.IsFrozen
              Parent = None
              IsAbstract = entityClass.IsAbstract
            }
        Some (name, entityClass, res)
    | _ -> failwith "Impossible"

// Build entities map using mish-mash of our custom attributes and Entity Framework Core declarations.
let buildSystemSchema (contextClass : Type) : SourceSchema =
    let entitiesInfo = contextClass.GetProperties() |> Seq.mapMaybe makeSourceEntity |> Seq.cache
    let types = entitiesInfo |> Seq.map (fun (name, propType, entity) -> (propType.FullName, name)) |> Map.ofSeq

    let applyParent (entity : SourceEntity) (propType : Type) =
        match propType.BaseType with
        | null as baseType | baseType when baseType = typeof<obj> -> entity
        | baseType ->
            { entity with Parent = Some { Schema = funSchema; Name = Map.find baseType.FullName types }}

    let entities = entitiesInfo |> Seq.map (fun (name, propType, entity) -> (name, applyParent entity propType)) |> Map.ofSeq
    { Entities = entities
    }