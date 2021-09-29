[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Set

let toMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
    s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

let getSingle (s : Set<'k>) : 'k = s |> Set.toSeq |> Seq.exactlyOne

let addUnique (x : 'k) (s : Set<'k>) : Set<'k> =
    if Set.contains x s then
        failwithf "Item '%O' already exists" x
    else
        Set.add x s

let ofSeqUnique (items : seq<'a>) : Set<'a> =
    Seq.fold (flip addUnique) Set.empty items

let unionUnique (a : Set<'k>) (b : Set<'k>) : Set<'k> =
    Seq.fold (flip addUnique) a b