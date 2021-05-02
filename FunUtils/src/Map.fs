[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Map

open System.Threading.Tasks

let addWith (resolve : 'v -> 'v -> 'v) (k : 'k) (v : 'v) (m : Map<'k, 'v>) : Map<'k, 'v> =
    let newValue =
        match Map.tryFind k m with
        | Some v' -> resolve v' v
        | None -> v
    Map.add k newValue m

let ofSeqWith (resolve : 'k -> 'v -> 'v -> 'v) (items : seq<'k * 'v>) : Map<'k, 'v> =
    Seq.fold (fun m (k, v) -> addWith (resolve k) k v m) Map.empty items

let ofSeqUnique (items : seq<'k * 'v>) : Map<'k, 'v> =
    ofSeqWith (fun k v1 v2 -> failwithf "Key '%O' already exists" k) items

let getWithDefault (k : 'k) (def : 'v) (m : Map<'k, 'v>) : 'v =
    match Map.tryFind k m with
    | Some v -> v
    | None -> def

let mapTask (f : 'k -> 'a -> Task<'b>) (m : Map<'k, 'a>) : Task<Map<'k, 'b>> =
    m |> Map.toSeq |> Seq.mapTask (fun (k, v) -> Task.map (fun nv -> (k, nv)) (f k v)) |> Task.map Map.ofSeq

let mapMaybe (f : 'k -> 'a -> 'b option) (m : Map<'k, 'a>) : Map<'k, 'b> =
    m |> Map.toSeq |> Seq.mapMaybe (fun (k, v) -> Option.map (fun v' -> (k, v')) (f k v)) |> Map.ofSeq

let unionWith (resolve : 'k -> 'v -> 'v -> 'v) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    ofSeqWith resolve (Seq.append (Map.toSeq a) (Map.toSeq b))

let unionUnique (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    ofSeqUnique (Seq.append (Map.toSeq a) (Map.toSeq b))

let union (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    Map.ofSeq (Seq.append (Map.toSeq a) (Map.toSeq b))

let intersectWithMaybe (resolve : 'k -> 'v -> 'v -> 'v1 option) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v1> =
    let intersectResolve m (k, v) =
        match Map.tryFind k b with
        | None -> m
        | Some v' ->
            match resolve k v v' with
            | None -> m
            | Some res -> Map.add k res m
    Seq.fold intersectResolve Map.empty (Map.toSeq a)

let intersectWith (resolve : 'k -> 'v -> 'v -> 'v1) (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v1> =
    let intersectResolve m (k, v) =
        match Map.tryFind k b with
        | None -> m
        | Some v' -> Map.add k (resolve k v v') m
    Seq.fold intersectResolve Map.empty (Map.toSeq a)

let intersect (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    intersectWith (fun k v1 v2 -> v2) a b

let difference (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
    Map.filter (fun k v -> not (Map.containsKey k b)) a

let singleton (k : 'k) (v : 'v) : Map<'k, 'v> = Seq.singleton (k, v) |> Map.ofSeq

let update (func : 'k -> 'a -> 'b option) (map : Map<'k, 'a>) : Map<'k, 'b> =
    map |> Map.toSeq |> Seq.mapMaybe (fun (k, a) -> Option.map (fun b -> (k, b)) <| func k a) |> Map.ofSeq

let keys (map : Map<'k, 'v>) : seq<'k> =
    map |> Map.toSeq |> Seq.map fst

let values (map : Map<'k, 'v>) : seq<'v> =
    map |> Map.toSeq |> Seq.map snd

let keysSet (map : Map<'k, 'v>) : Set<'k> =
    map |> keys |> Set.ofSeq

let mapWithKeys (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Map.toSeq |> Seq.map (fun (k, a) -> func k a) |> Map.ofSeq

let mapWithKeysWith (func : 'k1 -> 'a -> ('k2 * 'b)) (decide : 'k2 -> 'b -> 'b -> 'b) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Map.toSeq |> Seq.map (fun (k, a) -> func k a) |> ofSeqWith decide

let mapWithKeysUnique (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Map.toSeq |> Seq.map (fun (k, a) -> func k a) |> ofSeqUnique

let mapWithKeysMaybe (func : 'k1 -> 'a -> ('k2 * 'b) option) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
    map |> Map.toSeq |> Seq.mapMaybe (fun (k, a) -> func k a) |> Map.ofSeq

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

let findWithDefault (k : 'k) (def : unit -> 'v) (vals : Map<'k, 'v>) : 'v =
    match Map.tryFind k vals with
    | None -> def ()
    | Some v -> v

let reverse (map : Map<'k, 'v>): Map<'v, 'k> =
    map |> Map.toSeq |> Seq.map (fun (a, b) -> (b, a)) |> Map.ofSeq

let findOrFailWith (errorFunc : unit -> string) (k : 'k) (m : Map<'k, 'v>) : 'v =
    match Map.tryFind k m with
    | Some r -> r
    | None -> failwith (errorFunc ())
