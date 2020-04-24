module FunWithFlags.FunDB.Layout.System

open System
open System.Reflection
open System.ComponentModel.DataAnnotations

open FunWithFlags.FunDB.Utils
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
            { fieldType = field.Type
              defaultValue =
                  if isNull field.Default
                  then None
                  else Some field.Default
              isNullable = isNull requiredAttr && not property.PropertyType.IsPrimitive
              isImmutable = field.IsImmutable
            }
        Some (name, res)

let private makeSourceComputedField (field : ComputedFieldAttribute) : FunQLName * SourceComputedField =
    let res =
        { expression = field.Expression
          allowBroken = false
          isVirtual = field.IsVirtual
        }
    (FunQLName field.Name, res)

let private makeSourceUniqueConstraint (constr : UniqueConstraintAttribute) : FunQLName * SourceUniqueConstraint =
    let res = { columns = Array.map FunQLName constr.Columns }
    (FunQLName constr.Name, res)

let private makeSourceCheckConstraint (constr : CheckConstraintAttribute) : FunQLName * SourceCheckConstraint =
    let res = { expression = constr.Expression } : SourceCheckConstraint
    (FunQLName constr.Name, res)

let private getAttribute<'t when 't :> Attribute> (prop : PropertyInfo) =
    Attribute.GetCustomAttributes(prop, typeof<'t>) |> Array.map (fun x -> x :?> 't)

let private makeSourceEntity (prop : PropertyInfo) : (FunQLName * Type * SourceEntity) option =
    match Attribute.GetCustomAttribute(prop, typeof<EntityAttribute>) with
    | null -> None
    | :? EntityAttribute as entityAttr ->
        let name = FunQLName (snakeCaseName prop.Name)
        // Should be DbSet<Foo>
        let entityClass = prop.PropertyType.GetGenericArguments().[0]
        let columnFields = entityClass.GetProperties() |> Seq.mapMaybe makeSourceColumnField |> Map.ofSeq
        let computedFields = getAttribute<ComputedFieldAttribute> prop
        let uniqueConstraints = getAttribute<UniqueConstraintAttribute> prop
        let checkConstraints = getAttribute<CheckConstraintAttribute> prop

        let res =
            { columnFields = columnFields
              computedFields = computedFields |> Seq.map makeSourceComputedField |> Map.ofSeq
              uniqueConstraints = uniqueConstraints |> Seq.map makeSourceUniqueConstraint |> Map.ofSeq
              checkConstraints = checkConstraints |> Seq.map makeSourceCheckConstraint |> Map.ofSeq
              mainField = FunQLName entityAttr.MainField
              forbidExternalReferences = entityAttr.ForbidExternalReferences
              isHidden = entityAttr.IsHidden
              parent = None
              isAbstract = entityClass.IsAbstract
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
            { entity with parent = Some { schema = funSchema; name = Map.find baseType.FullName types }}

    let entities = entitiesInfo |> Seq.map (fun (name, propType, entity) -> (name, applyParent entity propType)) |> Map.ofSeq
    { entities = entities
    }