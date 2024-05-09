[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Map

open System.Linq
open System.Threading.Tasks
open FSharpPlus

let addWith (resolve : 'v -> 'v -> 'v) (k : 'k) (v : 'v) (m : Map<'k, 'v>) : Map<'k, 'v> =
    let newValue =
        match Map.tryFind k m with
        | Some v' -> resolve v' v
        | None -> v
    Map.add k newValue m

let addUnique (k : 'k) (v : 'v) (m : Map<'k, 'v>) : Map<'k, 'v> =
    addWith (fun v1 v2 -> failwithf "Key '%O' already exists" k) k v m

let ofSeqWith (resolve : 'k -> 'v -> 'v -> 'v) (items : seq<'k * 'v>) : Map<'k, 'v> =
    Seq.fold (fun m (k, v) -> addWith (resolve k) k v m) Map.empty items

let ofSeqUnique (items : seq<'k * 'v>) : Map<'k, 'v> =
    ofSeqWith (fun k v1 v2 -> failwithf "Key '%O' already exists" k) items

let mapTask (f : 'k -> 'a -> Task<'b>) (m : Map<'k, 'a>) : Task<Map<'k, 'b>> =
    m |> Seq.mapTask (fun (KeyValue(k, v)) -> Task.map (fun nv -> (k, nv)) (f k v)) |> Task.map Map.ofSeq

let mapMaybe (f : 'k -> 'a -> 'b option) (m : Map<'k, 'a>) : Map<'k, 'b> =
    m |> Seq.mapMaybe (fun (KeyValue(k, v)) -> Option.map (fun v' -> (k, v')) (f k v)) |> Map.ofSeq

let unionWithKey (resolve : 'k -> 'v -> 'v -> 'v) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    Map.fold (fun currA k v-> addWith (resolve k) k v currA) a b

let unionUnique (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    Map.fold (fun currA k v -> addUnique k v currA) a b

let intersectWithMaybe (resolve : 'k -> 'v -> 'v -> 'v1 option) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v1> =
    Enumerable.Join(a, b, (fun (KeyValue(k, v)) -> k), (fun (KeyValue(k, v)) -> k), fun (KeyValue(k1, v1)) (KeyValue(k2, v2)) -> Option.map (fun r -> (k1, r)) (resolve k1 v1 v2))
    |> Seq.catMaybes
    |> Map.ofSeq

let difference (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    Map.filter (fun k v -> not (Map.containsKey k b)) a

let differenceWithValues (isEqual : 'k -> 'v -> 'v -> bool) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    let filterOne k v1 =
        match Map.tryFind k b with
        | Some v2 -> not (isEqual k v1 v2)
        | None -> true
    Map.filter filterOne a

let singleton (k : 'k) (v : 'v) : Map<'k, 'v> = Map.add k v Map.empty

let update (func : 'k -> 'a -> 'b option) (map : Map<'k, 'a>) : Map<'k, 'b> =
    map |> Seq.mapMaybe (fun (KeyValue(k, a)) -> Option.map (fun b -> (k, b)) <| func k a) |> Map.ofSeq

let keysSet (map : Map<'k, 'v>) : Set<'k> =
    map |> Map.keys |> Set.ofSeq

let mapWithKeys (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Seq.map (fun (KeyValue(k, a)) -> func k a) |> Map.ofSeq

let mapWithKeysWith (func : 'k1 -> 'a -> ('k2 * 'b)) (decide : 'k2 -> 'b -> 'b -> 'b) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Seq.map (fun (KeyValue(k, a)) -> func k a) |> ofSeqWith decide

let mapWithKeysUnique (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Seq.map (fun (KeyValue(k, a)) -> func k a) |> ofSeqUnique

let mapWithKeysMaybe (func : 'k1 -> 'a -> ('k2 * 'b) option) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Seq.mapMaybe (fun (KeyValue(k, a)) -> func k a) |> Map.ofSeq

let mapKeys (func : 'k1 -> 'k2) : Map<'k1, 'a> -> Map<'k2, 'a> = mapWithKeys (fun name v -> (func name, v))

let traverseOption (func : 'k -> 'a -> 'b option) (vals : Map<'k, 'a>) : Map<'k, 'b> option =
    let res = vals |> Map.map func
    if Map.forall (fun key -> Option.isSome) res then
        Some (Map.map (fun key -> Option.get) res)
    else
        None

let traverseResult (func : 'k -> 'a -> Result<'b, 'e>) (vals : Map<'k, 'a>) : Result<Map<'k, 'b>, 'e> =
    let res = vals |> Map.map func
    match res |> Map.toSeq |> Seq.filter (fun (_, r) -> Result.isError r) |> Seq.first with
    | Some (_, err) -> Error <| Result.getError err
    | None -> Ok <| Map.map (fun key -> Result.get) res

let findWithDefault (k : 'k) (def : 'v) (vals : Map<'k, 'v>) : 'v =
    match Map.tryFind k vals with
    | None -> def
    | Some v -> v

let findWithLazyDefault (k : 'k) (def : unit -> 'v) (vals : Map<'k, 'v>) : 'v =
    match Map.tryFind k vals with
    | None -> def ()
    | Some v -> v

let reverse (map : Map<'k, 'v>): Map<'v, 'k> =
    map |> Seq.map (fun (KeyValue(a, b)) -> (b, a)) |> Map.ofSeq

let reverseWith (resolve : 'v -> 'k -> 'k -> 'k) (map : Map<'k, 'v>): Map<'v, 'k> =
    map |> Seq.map (fun (KeyValue(a, b)) -> (b, a)) |> ofSeqWith resolve

let findOrFailWith (errorFunc : unit -> string) (k : 'k) (m : Map<'k, 'v>) : 'v =
    match Map.tryFind k m with
    | Some r -> r
    | None -> failwith (errorFunc ())

let partitionEither (f : 'k -> 'v -> Choice<'a, 'b>) (m : Map<'k, 'v>) : Map<'k, 'a> * Map<'k, 'b> =
    let mutable map1 = Map.empty
    let mutable map2 = Map.empty
    for KeyValue(name, value) in m do
        match f name value with
        | Choice1Of2 v -> map1 <- Map.add name v map1
        | Choice2Of2 v -> map2 <- Map.add name v map2
    (map1, map2)