module FunWithFlags.FunDB.Layout.System

open System
open System.Reflection
open System.ComponentModel.DataAnnotations.Schema

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Layout.Source

[<AttributeUsage(AttributeTargets.Field)>]
type EntityAttribute () =
    inherit Attribute ()
    member val Abstract = false with get, set
    member val Ancestor = None : string option with get, set

[<AttributeUsage(AttributeTargets.Field, AllowMultiple=true)>]
type UniqueConstraintAttribute (name : string, columns : string array) =
    inherit Attribute ()
    member this.Name = name
    member this.Columns = columns

[<AttributeUsage(AttributeTargets.Field, AllowMultiple=true)>]
type CheckConstraintAttribute (name : string, expression : string) =
    inherit Attribute ()
    member this.Name = name
    member this.Expression = expression

[<AttributeUsage(AttributeTargets.Field)>]
type ColumnFieldAttribute (colType : string) =
    inherit Attribute ()
    member this.Type = colType
    member val Nullable = false with get, set
    member val Default = None : string option with get, set

let private makeSourceField (property : PropertyInfo) : (FunQLName * SourceColumnField) option =
    let field = Attribute.GetCustomAttribute(property, typeof<ColumnFieldAttribute>) :?> ColumnFieldAttribute
    if field = null
    then None
    else
        let res =
            { fieldType = field.Type
              defaultExpr = field.Default
              isNullable = field.Nullable
            }
        Some (FunQLName name, res)

let private makeSourceUniqueConstraint (constr : UniqueConstraintAttribute) : FunQLName * SourceUniqueConstraint =
    let res = { columns = Array.map FunQLName constr.Columns }
    (FunQLName constr.Name, res)

let private makeSourceCheckConstraint (constr : CheckConstraintAttribute) : FunQLName * SourceCheckConstraint =
    let res = { expression = constr.Expression }
    (FunQLName constr.Name, res)

let private makeSourceEntity (property : PropertyInfo) : (FunQLName * SourceEntity) option =
    let entityAttr = Attribute.GetCustomAttribute(property, typeof<EntityAttribute>) :?> EntityAttribute
    if entityAttr = null
    then None
    else
        // Should be DbSet<Foo>
        let entityClass = property.PropertyType.GetGenericArguments().[0]
        let fields = entityClass.GetProperties() |> seqMapMaybe makeSourceField |> Map.ofSeq
        let uniqueConstraints = Attribute.GetCustomAttributes(property, typeof<UniqueConstraintAttribute>) :?> UniqueConstraintAttribute array
        let checkConstraints = Attribute.GetCustomAttributes(property, typeof<CheckConstraintAttribute>) :?> CheckConstraintAttribute array

        let res =
            { columnFields = fields
              computedFields = Map.empty
              uniqueConstraints = uniqueConstraints |> Seq.map makeSourceUniqueConstraint |> Map.ofSeq
              checkConstraints = checkConstraints |> Seq.map makeSourceCheckConstraint |> Map.ofSeq
            }
        Some (FunQLName property.Name, res)

// Build entities map for public schema using mish-mash of our custom attributes and Entity Framework Core declarations.
let buildSystemLayout (contextClass : Type) : SourceLayout = 
    { schemas = Map.empty
      systemEntities = contextClass.GetProperties() |> seqMapMaybe buildEntityMeta |> Map.ofSeq
    }
