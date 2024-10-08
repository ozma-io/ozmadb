namespace OzmaDB.OzmaUtils

open System
open System.Collections
open System.Collections.Generic

type OrderedMap<'k, 'v> when 'k: comparison =
    private
        { Order: 'k[]
          Map: Map<'k, 'v> }

    override this.ToString() =
        this.Order
        |> Seq.map (fun k -> string (k, Map.find k this.Map))
        |> String.concat ", "
        |> sprintf "[%s]"

    member this.Count = this.Order.Length

    member this.GetEnumerator() =
        let ret = this.Order |> Seq.map (fun k -> KeyValuePair(k, Map.find k this.Map))
        ret.GetEnumerator()

    member this.CopyTo(arr: KeyValuePair<'k, 'v>[], arrayIndex: int) =
        for i = 0 to this.Count - 1 do
            let k = this.Order.[i]
            let v = Map.find k this.Map
            arr.[arrayIndex + i] <- KeyValuePair(k, v)

    member this.Item
        with get key = Map.find key this.Map

    member this.Keys = Array.copy this.Order :> ICollection<'k>

    member this.Values =
        [| for k in this.Order -> Map.find k this.Map |] :> ICollection<'v>

    member this.ContainsKey key = Map.containsKey key this.Map

    member this.TryGetValue(key, [<System.Runtime.InteropServices.Out>] value: byref<'v>) =
        this.Map.TryGetValue(key, &value)

    interface IEnumerable with
        member this.GetEnumerator() = this.GetEnumerator()

    interface IEnumerable<KeyValuePair<'k, 'v>> with
        member this.GetEnumerator() = this.GetEnumerator()

    interface ICollection<KeyValuePair<'k, 'v>> with
        member this.Count = this.Count
        member this.IsReadOnly = true
        member this.Add _ = raise <| NotSupportedException()
        member this.Clear() = raise <| NotSupportedException()

        member this.Contains pair =
            (this.Map :> ICollection<KeyValuePair<'k, 'v>>).Contains pair

        member this.CopyTo(arr, arrayIndex) = this.CopyTo(arr, arrayIndex)
        member this.Remove _ = raise <| NotSupportedException()

    interface IReadOnlyCollection<KeyValuePair<'k, 'v>> with
        member this.Count = this.Count

    interface IDictionary<'k, 'v> with
        member this.Item
            with get key = this.[key]
            and set _ _ = raise <| NotSupportedException()

        member this.Keys = this.Keys
        member this.Values = this.Values
        member this.Add(_, _) = raise <| NotSupportedException()
        member this.ContainsKey key = this.ContainsKey key
        member this.Remove _ = raise <| NotSupportedException()
        member this.TryGetValue(key, value) = this.TryGetValue(key, &value)

    interface IReadOnlyDictionary<'k, 'v> with
        member this.Item
            with get key = this.[key]

        member this.Keys = this.Keys
        member this.Values = this.Values
        member this.ContainsKey key = this.ContainsKey key
        member this.TryGetValue(key, value) = this.TryGetValue(key, &value)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module OrderedMap =
    let empty: OrderedMap<'k, 'v> = { Order = [||]; Map = Map.empty }

    let size (map: OrderedMap<'k, 'v>) : int = Map.count map.Map

    let isEmpty (map: OrderedMap<'k, 'v>) : bool = Map.isEmpty map.Map

    let inline private ofSeqGeneric (onDuplicate: 'k -> 'v -> unit) (objs: ('k * 'v) seq) : OrderedMap<'k, 'v> =
        let order = List()
        let mutable keys = Map.empty

        for (k, v) in objs do
            if Map.containsKey k keys then
                onDuplicate k v
            else
                order.Add(k)
                keys <- Map.add k v keys

        { Order = order.ToArray(); Map = keys }

    let ofSeq (objs: ('k * 'v) seq) = ofSeqGeneric (fun _ _ -> ()) objs

    let ofSeqUnique (objs: ('k * 'v) seq) =
        ofSeqGeneric (fun key value -> failwithf "Key '%O' already exists" key) objs

    let ofMap (smap: Map<'k, 'v>) : OrderedMap<'k, 'v> =
        { Order = smap |> Map.keys |> Seq.toArray
          Map = smap }

    let singleton (key: 'k) (value: 'v) : OrderedMap<'k, 'v> =
        { Order = [| key |]
          Map = Map.singleton key value }

    let keys (map: OrderedMap<'k, 'v>) : 'k seq = map.Order

    let values (map: OrderedMap<'k, 'v>) : 'v seq =
        keys map |> Seq.map (fun k -> Map.find k map.Map)

    let toSeq (map: OrderedMap<'k, 'v>) : ('k * 'v) seq =
        keys map |> Seq.map (fun k -> (k, Map.find k map.Map))

    let toMap (map: OrderedMap<'k, 'v>) : Map<'k, 'v> = map.Map

    let filter (f: 'k -> 'v -> bool) (map: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        let mutable newKeys = map.Map

        let checkKey k =
            if f k (Map.find k newKeys) then
                true
            else
                newKeys <- Map.remove k newKeys
                false

        let newOrder = Array.filter checkKey map.Order

        { Order = newOrder; Map = newKeys }

    let add (key: 'k) (value: 'v) (map: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        if Map.containsKey key map.Map then
            { Order = map.Order
              Map = Map.add key value map.Map }
        else
            { Order = Array.add key map.Order
              Map = Map.add key value map.Map }

    let addWith (resolve: 'v -> 'v -> 'v) (key: 'k) (value: 'v) (map: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        match Map.tryFind key map.Map with
        | None ->
            { Order = Array.add key map.Order
              Map = Map.add key value map.Map }
        | Some oldValue ->
            { Order = map.Order
              Map = Map.add key (resolve oldValue value) map.Map }

    let addUnique (k: 'k) (v: 'v) (m: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        addWith (fun v1 v2 -> failwithf "Key '%O' already exists" k) k v m

    let remove (key: 'k) (map: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        match Array.tryFindIndex (fun ckey -> ckey = key) map.Order with
        | None -> map
        | Some i ->
            { Order = Array.removeAt i map.Order
              Map = Map.remove key map.Map }

    let map (f: 'k -> 'v1 -> 'v2) (map: OrderedMap<'k, 'v1>) : OrderedMap<'k, 'v2> =
        { Order = map.Order
          Map = Map.map f map.Map }

    let mapWithKeys (f: 'k1 -> 'v1 -> ('k2 * 'v2)) (map: OrderedMap<'k1, 'v1>) : OrderedMap<'k2, 'v2> =
        let mapOne k = f k (Map.find k map.Map)
        let newValues = Array.map mapOne map.Order

        { Order = Array.map fst newValues
          Map = Map.ofArray newValues }

    let mapMaybe (f: 'k -> 'v1 -> 'v2 option) (map: OrderedMap<'k, 'v1>) : OrderedMap<'k, 'v2> =
        let mapOne k =
            Option.map (fun value -> (k, value)) <| f k (Map.find k map.Map)

        let newValues = Seq.mapMaybe mapOne map.Order |> Array.ofSeq

        { Order = Array.map fst newValues
          Map = Map.ofArray newValues }

    let mapWithKeysMaybe (f: 'k1 -> 'v1 -> ('k2 * 'v2) option) (map: OrderedMap<'k1, 'v1>) : OrderedMap<'k2, 'v2> =
        let mapOne k = f k (Map.find k map.Map)
        let newValues = Seq.mapMaybe mapOne map.Order |> Array.ofSeq

        { Order = Array.map fst newValues
          Map = Map.ofArray newValues }

    let mapKeys (f: 'k1 -> 'k2) (map: OrderedMap<'k1, 'v>) : OrderedMap<'k2, 'v> =
        let mutable newKeys = Map.empty

        let mapOne k =
            let newK = f k
            newKeys <- Map.add newK (Map.find k map.Map) newKeys
            newK

        let newOrder = Array.map f map.Order

        { Order = newOrder; Map = newKeys }

    let union (map1: OrderedMap<'k, 'v>) (map2: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        map1 |> toSeq |> Seq.fold (fun cmap (k, v) -> add k v cmap) map2

    let unionUnique (map1: OrderedMap<'k, 'v>) (map2: OrderedMap<'k, 'v>) : OrderedMap<'k, 'v> =
        map1 |> toSeq |> Seq.fold (fun cmap (k, v) -> addUnique k v cmap) map2

    let containsKey (key: 'k) (map: OrderedMap<'k, 'v>) : bool = Map.containsKey key map.Map

    let tryFind (key: 'k) (map: OrderedMap<'k, 'v>) : 'v option = Map.tryFind key map.Map

    let find (key: 'k) (map: OrderedMap<'k, 'v>) : 'v = Map.find key map.Map
