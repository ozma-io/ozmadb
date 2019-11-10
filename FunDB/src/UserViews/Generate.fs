module FunWithFlags.FunDB.UserViews.Generate

open System.Globalization
open Newtonsoft.Json
open Jint.Native
open Jint.Runtime.Descriptors
open Jint.Runtime.Interop

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Render
open FunWithFlags.FunDB.SQL.Utils

type UserViewGenerateException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewGenerateException (message, null)

let userViewsFunction = "GetUserViews"

[<NoComparison>]
type SerializedColumnField =
    { fieldType : string
      defaultValue : string option
      isNullable : bool
      isImmutable : bool
      inheritedFrom : ResolvedEntityRef option
    }

[<NoComparison>]
type SerializedComputedField =
    { expression : string
      // Set when there's no dereferences in the expression
      isLocal : bool
      // Set when computed field uses Id
      hasId : bool
      usedSchemas : UsedSchemas
      inheritedFrom : ResolvedEntityRef option
    }

[<NoComparison>]
type SerializedEntity =
    { columnFields : Map<FieldName, SerializedColumnField>
      computedFields : Map<FieldName, SerializedComputedField>
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      forbidExternalReferences : bool
      hidden : bool
      parent : ResolvedEntityRef option
      children : Set<ResolvedEntityRef>
      isAbstract : bool
      root : ResolvedEntityRef
    }

[<NoComparison>]
type SerializedSchema =
    { entities : Map<EntityName, SerializedEntity>
      roots : Set<EntityName>
      forbidExternalInheritance : bool
    }

[<NoComparison>]
type SerializedLayout =
    { schemas : Map<SchemaName, SerializedSchema>
    }

let private serializeComputedField (comp : ResolvedComputedField) : SerializedComputedField =
    { expression = comp.expression.ToFunQLString()
      isLocal = comp.isLocal
      hasId = comp.hasId
      usedSchemas = comp.usedSchemas
      inheritedFrom = comp.inheritedFrom
    }

let private serializeColumnField (column : ResolvedColumnField) : SerializedColumnField =
    { fieldType = column.fieldType.ToFunQLString()
      isNullable = column.isNullable
      isImmutable = column.isImmutable
      defaultValue = Option.map (fun (x : FieldValue) -> x.ToFunQLString()) column.defaultValue
      inheritedFrom = column.inheritedFrom
    }

let private serializeEntity (entity : ResolvedEntity) : SerializedEntity =
    { columnFields = Map.map (fun name col -> serializeColumnField col) entity.columnFields
      computedFields = Map.map (fun name comp -> serializeComputedField comp) entity.computedFields
      uniqueConstraints = Map.map (fun name constr -> renderUniqueConstraint constr) entity.uniqueConstraints
      checkConstraints = Map.map (fun name constr -> renderCheckConstraint constr) entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      hidden = entity.hidden
      parent = entity.inheritance |> Option.map (fun inher -> inher.parent)
      isAbstract = entity.isAbstract
      children = entity.children
      root = entity.root
    }

let private serializeSchema (schema : ResolvedSchema) : SerializedSchema =
    { entities = Map.map (fun name entity -> serializeEntity entity) schema.entities
      forbidExternalInheritance = schema.forbidExternalInheritance
      roots = schema.roots
    }

let private serializeLayout (layout : Layout) : SerializedLayout =
    { schemas = Map.map (fun name schema -> serializeSchema schema) layout.schemas
    }

let private convertUserView (KeyValue (k, v : PropertyDescriptor)) =
    let query = Jint.Runtime.TypeConverter.ToString(v.Value)
    let uv =
        { query = query
          allowBroken = false
        }
    (FunQLName k, uv)

type UserViewGenerator (jsProgram : string) =
    let engine =
        let engine = Jint.Engine(fun cfg -> ignore <| cfg.Culture(CultureInfo.InvariantCulture)).Execute(jsProgram)

        let mutable userViewsFunctionKey = Jint.Key userViewsFunction
        if not <| engine.Global.HasProperty(&userViewsFunctionKey) then
            raisef UserViewGenerateException "Function %O is undefined" userViewsFunction

        // XXX: reimplement this in JavaScript for performance if we switch to V8
        let jsRenderSqlName (this : JsValue) (args : JsValue[]) =
            args.[0] |> Jint.Runtime.TypeConverter.ToString |> renderSqlName |> JsString :> JsValue
        // Strange that this is needed...
        let jsRenderSqlNameF = System.Func<JsValue, JsValue[], JsValue>(jsRenderSqlName)
        engine.Global.FastAddProperty("renderSqlName", ClrFunctionInstance(engine, "renderSqlName", jsRenderSqlNameF, 1), true, false, true)

        engine

    let generateUserViews (layout : Layout) : SourceUserViewsSchema =
        let newViews =
            lock engine <| fun () ->
                // Better way?
                let jsonParser = Jint.Native.Json.JsonParser(engine)
                let jsLayout = layout |> serializeLayout |> JsonConvert.SerializeObject |> jsonParser.Parse
                engine.Invoke(userViewsFunction, jsLayout)

        let userViews = newViews.AsObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
        { userViews = userViews
        }

    member this.Generate = generateUserViews