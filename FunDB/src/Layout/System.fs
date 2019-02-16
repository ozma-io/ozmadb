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
    member val Default = null : string with get, set

let private makeSourceColumnField (property : PropertyInfo) : (FunQLName * SourceColumnField) option =
    let field = Attribute.GetCustomAttribute(property, typeof<ColumnFieldAttribute>) :?> ColumnFieldAttribute
    if isNull field
    then None
    else
        let res =
            { fieldType = field.Type
              defaultValue =
                  if field.Default = null
                  then None
                  else Some field.Default
              isNullable = field.Nullable
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

let private makeSourceEntity (prop : PropertyInfo) : (FunQLName * SourceEntity) option =
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
            }
        Some (FunQLName prop.Name, res)

// Build entities map for public schema using mish-mash of our custom attributes and Entity Framework Core declarations.
let buildSystemLayout (contextClass : Type) : SourceLayout =
    let entities = contextClass.GetProperties() |> Seq.mapMaybe makeSourceEntity |> Map.ofSeq
    { schemas = Map.singleton funSchema { entities = entities }
    }
