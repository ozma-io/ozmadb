namespace OzmaDB.OzmaUtils

open System.Collections.Generic
open System.Collections.Immutable

type HashMap<'k, 'v> = ImmutableDictionary<'k, 'v>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module HashMap =
    let empty<'k, 'v> : HashMap<'k, 'v> = ImmutableDictionary.Create()

    let size (map: HashMap<'k, 'v>) : int = map.Count

    let isEmpty (map: HashMap<'k, 'v>) : bool = map.IsEmpty

    let ofSeq (objs: ('k * 'v) seq) : HashMap<'k, 'v> =
        objs
        |> Seq.map (fun (k, v) -> KeyValuePair(k, v))
        |> ImmutableDictionary.CreateRange

    let ofSeqWith (f: 'k -> 'v -> 'v -> 'v) (objs: ('k * 'v) seq) : HashMap<'k, 'v> =
        let builder = ImmutableDictionary.CreateBuilder<'k, 'v>()

        for (key, value) in objs do
            match builder.TryGetValue key with
            | (true, oldValue) -> builder.[key] <- f key oldValue value
            | _ -> builder.Add(key, value)

        ImmutableDictionary.ToImmutableDictionary builder

    let ofSeqUnique (items: seq<'k * 'v>) : HashMap<'k, 'v> =
        ofSeqWith (fun k v1 v2 -> failwithf "Key '%O' already exists" k) items

    let singleton (key: 'k) (value: 'v) : HashMap<'k, 'v> =
        KeyValuePair(key, value) |> Seq.singleton |> ImmutableDictionary.CreateRange

    let toSeq (map: HashMap<'k, 'v>) : ('k * 'v) seq =
        map |> Seq.map (fun (KeyValue(k, v)) -> (k, v))

    let map (f: 'k -> 'v1 -> 'v2) (map: HashMap<'k, 'v1>) : HashMap<'k, 'v2> =
        let builder = ImmutableDictionary.CreateBuilder<'k, 'v2>()

        for KeyValue(k, v) in map do
            builder.[k] <- f k v

        builder.ToImmutable()

    let mapWithKeys (f: 'k1 -> 'v1 -> ('k2 * 'v2)) (map: HashMap<'k1, 'v1>) : HashMap<'k2, 'v2> =
        let builder = ImmutableDictionary.CreateBuilder<'k2, 'v2>()

        for KeyValue(k, v) in map do
            let (newK, newV) = f k v
            builder.[newK] <- newV

        builder.ToImmutable()

    let filter (f: 'k -> 'v -> bool) (map: HashMap<'k, 'v>) : HashMap<'k, 'v> =
        map
        |> Seq.filter (fun (KeyValue(k, v)) -> f k v)
        |> ImmutableDictionary.CreateRange

    let add (key: 'k) (value: 'v) (map: HashMap<'k, 'v>) : HashMap<'k, 'v> =
        let builder = map.ToBuilder()
        builder.[key] <- value
        builder.ToImmutable()

    let addWith (resolve: 'v -> 'v -> 'v) (k: 'k) (v: 'v) (map: HashMap<'k, 'v>) : HashMap<'k, 'v> =
        match map.TryGetValue k with
        | (true, oldValue) ->
            let builder = map.ToBuilder()
            builder.[k] <- resolve oldValue v
            builder.ToImmutable()
        | _ -> map.Add(k, v)

    let addUnique (k: 'k) (v: 'v) (m: HashMap<'k, 'v>) : HashMap<'k, 'v> =
        addWith (fun v1 v2 -> failwithf "Key '%O' already exists" k) k v m

    let remove (key: 'k) (map: HashMap<'k, 'v>) : HashMap<'k, 'v> = map.Remove(key)

    let tryFind (key: 'k) (map: HashMap<'k, 'v>) : 'v option =
        match map.TryGetValue key with
        | (true, obj) -> Some obj
        | _ -> None

    let find (key: 'k) (map: HashMap<'k, 'v>) : 'v = map.[key]

    let containsKey (key: 'k) (map: HashMap<'k, 'v>) : bool = map.ContainsKey key
