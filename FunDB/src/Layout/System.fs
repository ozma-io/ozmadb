module FunWithFlags.FunDB.Layout.System

open System
open System.Reflection

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.FunQL.AST

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Property)>]
type EntityAttribute (mainField : string) =
    inherit Attribute ()
    member this.MainField = mainField
    member val ForbidExternalReferences = false with get, set
    member val Hidden = false with get, set
    member val IsAbstract = false with get, set

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Property, AllowMultiple=true)>]
type UniqueConstraintAttribute (name : string, columns : string array) =
    inherit Attribute ()
    member this.Name = name
    member this.Columns = columns

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Property, AllowMultiple=true)>]
type CheckConstraintAttribute (name : string, expression : string) =
    inherit Attribute ()
    member this.Name = name
    member this.Expression = expression

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Property, AllowMultiple=true)>]
type ComputedFieldAttribute (name : string, expression : string) =
    inherit Attribute ()
    member this.Name = name
    member this.Expression = expression

[<AllowNullLiteral>]
[<AttributeUsage(AttributeTargets.Property)>]
type ColumnFieldAttribute (colType : string) =
    inherit Attribute ()
    member this.Type = colType
    member val Nullable = false with get, set
    member val Immutable = false with get, set
    member val Default = null : string with get, set

let private makeSourceColumnField (property : PropertyInfo) : (FunQLName * SourceColumnField) option =
    let field = Attribute.GetCustomAttribute(property, typeof<ColumnFieldAttribute>) :?> ColumnFieldAttribute
    // Check also that property is not inherited.
    if property.DeclaringType <> property.ReflectedType || isNull field
    then None
    else
        let res =
            { fieldType = field.Type
              defaultValue =
                  if field.Default = null
                  then None
                  else Some field.Default
              isNullable = field.Nullable
              isImmutable = field.Immutable
            }
        Some (FunQLName property.Name, res)

let private makeSourceComputedField (field : ComputedFieldAttribute) : FunQLName * SourceComputedField =
    let res = { expression = field.Expression }
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
    let entityAttr = Attribute.GetCustomAttribute(prop, typeof<EntityAttribute>) :?> EntityAttribute
    if isNull entityAttr
    then None
    else
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
              hidden = entityAttr.Hidden
              parent = None
              isAbstract = entityAttr.IsAbstract
            }
        Some (FunQLName prop.Name, entityClass, res)

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
      forbidExternalInheritance = true
    }