module FunWithFlags.FunUtils.Set

let toMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
    s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

let getSingle (s : Set<'k>) : 'k = s |> Set.toSeq |> Seq.exactlyOne

let ofSeqUnique (items : seq<'a>) : Set<'a> =
    Seq.fold (fun s x -> if Set.contains x s then failwith (sprintf "Item '%O' already exists" x) else Set.add x s) Set.empty items
