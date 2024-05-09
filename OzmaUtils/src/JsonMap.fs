namespace OzmaDB.OzmaUtils

open System
open System.Collections
open System.Collections.Generic
open System.ComponentModel
open Newtonsoft.Json
open Newtonsoft.Json.Linq

open OzmaDB.OzmaUtils.Reflection


[<TypeConverter(typeof<NewtypeConverter<JsonMap>>)>]
[<Struct>]
[<CustomEquality>]
[<NoComparison>]
type JsonMap = JsonMap of JObject
    with
        static member private Comparer = JTokenEqualityComparer()
        member this.Map =
            match this with
            | JsonMap map -> map

        override this.Equals other =
            match other with
            | :? JsonMap as other -> JObject.DeepEquals(this.Map, other.Map)
            | _ -> false

        override this.GetHashCode () = JsonMap.Comparer.GetHashCode(this.Map)

        interface IEquatable<JsonMap> with
            member this.Equals (other : JsonMap) =
                JObject.DeepEquals(this.Map, other.Map)

        override this.ToString () = this.Map.ToString()

        member this.Count = this.Map.Count

        member this.IsEmpty = not this.Map.HasValues

        member this.GetEnumerator () = this.Map.GetEnumerator()

        member this.Item
            with get key = this.Map.[key]

        member this.ContainsKey key = this.Map.ContainsKey key

        member this.TryGetValue (key, [<System.Runtime.InteropServices.Out>] value : byref<JToken>) =
            this.Map.TryGetValue(key, &value)

        interface IEnumerable with
            member this.GetEnumerator () = this.GetEnumerator ()

        interface IEnumerable<KeyValuePair<string, JToken>> with
            member this.GetEnumerator () = this.GetEnumerator ()

        interface ICollection<KeyValuePair<string, JToken>> with
            member this.Count = this.Count
            member this.IsReadOnly = true
            member this.Add _ = raise <| NotSupportedException ()
            member this.Clear () = raise <| NotSupportedException ()
            member this.Contains pair =
                (this.Map :> ICollection<KeyValuePair<string, JToken>>).Contains pair
            member this.CopyTo (arr, arrayIndex) =
                (this.Map :> ICollection<KeyValuePair<string, JToken>>).CopyTo (arr, arrayIndex)
            member this.Remove _ = raise <| NotSupportedException ()

        interface IReadOnlyCollection<KeyValuePair<string, JToken>> with
            member this.Count = this.Count

        interface IDictionary<string, JToken> with
            member this.Item
                with get key = this.[key]
                and set _ _ = raise <| NotSupportedException ()
            member this.Keys = (this.Map :> IDictionary<string, JToken>).Keys
            member this.Values = (this.Map :> IDictionary<string, JToken>).Values
            member this.Add (_, _) = raise <| NotSupportedException ()
            member this.ContainsKey key = this.ContainsKey key
            member this.Remove _ = raise <| NotSupportedException ()
            member this.TryGetValue (key, value) = this.TryGetValue (key, &value)

        interface IReadOnlyDictionary<string, JToken> with
            member this.Item
                with get key = this.[key]
            member this.Keys = (this.Map :> IDictionary<string, JToken>).Keys
            member this.Values = (this.Map :> IDictionary<string, JToken>).Values
            member this.ContainsKey key = this.ContainsKey key
            member this.TryGetValue (key, value) = this.TryGetValue (key, &value)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module JsonMap =
    let empty : JsonMap = JsonMap (JObject())

    let parse (str : string) = JsonMap (JObject.Parse str)

    let fromObject (o : obj) = JsonMap (JObject.FromObject o)

    let toJSON (map : JsonMap) = map.Map.ToString(Formatting.None)

    let size (map : JsonMap) : int = map.Map.Count

    let isEmpty (map : JsonMap) : bool = map.Map.HasValues

    let ofSeq (items : (string * JToken) seq) : JsonMap =
        let obj = JObject()
        for (key, value) in items do
            obj.Add(key, value)
        JsonMap obj

    let ofSeqUnique (items : (string * JToken) seq) : JsonMap =
        let obj = JObject()
        for (key, value) in items do
            if obj.ContainsKey key then
                failwithf "Item '%s' already exists" key
            obj.Add(key, value)
        JsonMap obj

    let singleton (key : string) (item : JToken) : JsonMap =
        ofSeq (seq { yield key, item })

    let toSeq (map : JsonMap) : (string * JToken) seq =
        map.Map.Properties() |> Seq.map (fun p -> (p.Name, p.Value))

    let contains (key : string) (map : JsonMap) : bool =
        map.Map.ContainsKey key
