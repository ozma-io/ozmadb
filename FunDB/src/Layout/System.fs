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
[<AttributeUsage(AttributeTargets.Property)>]
type ColumnFieldAttribute (colType : string) =
    inherit Attribute ()
    member this.Type = colType
    member val Nullable = false with get, set
    member val Default = null : string with get, set

let private makeSourceField (property : PropertyInfo) : (FunQLName * SourceColumnField) option =
    let field = Attribute.GetCustomAttribute(property, typeof<ColumnFieldAttribute>) :?> ColumnFieldAttribute
    if field = null
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

let private makeSourceUniqueConstraint (constr : UniqueConstraintAttribute) : FunQLName * SourceUniqueConstraint =
    let res = { columns = Array.map FunQLName constr.Columns }
    (FunQLName constr.Name, res)

let private makeSourceCheckConstraint (constr : CheckConstraintAttribute) : FunQLName * SourceCheckConstraint =
    let res = { expression = constr.Expression } : SourceCheckConstraint
    (FunQLName constr.Name, res)

let private makeSourceEntity (property : PropertyInfo) : (FunQLName * SourceEntity) option =
    let entityAttr = Attribute.GetCustomAttribute(property, typeof<EntityAttribute>) :?> EntityAttribute
    if entityAttr = null
    then None
    else
        // Should be DbSet<Foo>
        let entityClass = property.PropertyType.GetGenericArguments().[0]
        let fields = entityClass.GetProperties() |> Seq.mapMaybe makeSourceField |> Map.ofSeq
        let uniqueConstraints = Attribute.GetCustomAttributes(property, typeof<UniqueConstraintAttribute>) |> Array.map (fun x -> x :?> UniqueConstraintAttribute)
        let checkConstraints = Attribute.GetCustomAttributes(property, typeof<CheckConstraintAttribute>) |> Array.map (fun x -> x :?> CheckConstraintAttribute)

        let res =
            { columnFields = fields
              computedFields = Map.empty
              uniqueConstraints = uniqueConstraints |> Seq.map makeSourceUniqueConstraint |> Map.ofSeq
              checkConstraints = checkConstraints |> Seq.map makeSourceCheckConstraint |> Map.ofSeq
              mainField = FunQLName entityAttr.MainField
            }
        Some (FunQLName property.Name, res)

// Build entities map for public schema using mish-mash of our custom attributes and Entity Framework Core declarations.
let buildSystemLayout (contextClass : Type) : SourceLayout = 
    { schemas = Map.empty
      systemEntities = contextClass.GetProperties() |> Seq.mapMaybe makeSourceEntity |> Map.ofSeq
    }
