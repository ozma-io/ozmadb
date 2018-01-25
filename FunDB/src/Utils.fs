module FunWithFlags.FunDB.Utils

type Void = private Void of unit

let mapGetWithDefault (k : 'K) (def : 'V) (m : Map<'K, 'V>) : 'V =
    match Map.tryFind k m with
        | Some(v) -> v
        | None -> def

let setToMap (f : 'K -> 'V) (s : Set<'K>) : Map<'K, 'V> =
    s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

let setHead (s : Set<'K>) : 'K =
    match Set.toList s with
        | [] -> failwith "Empty set"
        | [k] -> k
        | _ -> failwith "Set consists of more than one value"

let seqMapMaybe (f : 'A -> 'B option) (s : seq<'A>) : seq<'B> =
    seq { for i in s do
              match f i with
                  | Some(r) -> yield r
                  | None -> ()
        }

let mapOfSeqUnique (items : seq<'K * 'V>) : Map<'K, 'V> =
    Seq.fold (fun m (k, v) -> if Map.containsKey k m then failwith (sprintf "Key '%s' already exists" (k.ToString ())) else Map.add k v m) Map.empty items

let setOfSeqUnique (items : seq<'T>) : Set<'T> =
    Seq.fold (fun s x -> if Set.contains x s then failwith (sprintf "Item '%s' already exists" (x.ToString ())) else Set.add x s) Set.empty items

let mapUnionUnique (a : Map<'K, 'V>) (b : Map<'K, 'V>) =
    mapOfSeqUnique (Seq.append (Map.toSeq a) (Map.toSeq b))

let mapUnion (a : Map<'K, 'V>) (b : Map<'K, 'V>) =
    Map.ofSeq (Seq.append (Map.toSeq a) (Map.toSeq b))

let mapDifference (a : Map<'K, 'V1>) (b : Map<'K, 'V2>) =
    Map.filter (fun k v -> not (Map.containsKey k b)) a

let mapSingleton (k : 'K) (v : 'V) : Map<'K, 'V> =
    Map.ofSeq (seq { yield (k, v) })

let inline combineHash (a : int) (b : int) : int =  31 * (31 * a + 23) + b

let inline combineCompare (a : int) (b : int) : int = if a <> 0 then a else b
