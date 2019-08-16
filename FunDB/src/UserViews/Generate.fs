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
open FunWithFlags.FunDB.Layout.Render
open FunWithFlags.FunDB.SQL.Utils

type UserViewGenerateException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = UserViewGenerateException (message, null)

let userViewsFunction = "GetUserViews"

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
                let jsLayout = layout |> renderLayout |> JsonConvert.SerializeObject |> jsonParser.Parse
                engine.Invoke(userViewsFunction, jsLayout)

        let userViews = newViews.AsObject().GetOwnProperties() |> Seq.map convertUserView |> Map.ofSeq
        { userViews = userViews
        }

    member this.Generate = generateUserViews