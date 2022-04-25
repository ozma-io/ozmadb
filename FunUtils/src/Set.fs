[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Set

open FSharpPlus

let toMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
    s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

let getSingle (s : Set<'k>) : 'k = s |> Set.toSeq |> Seq.exactlyOne

let private addUnique (x : 'k) (s : Set<'k>) : Set<'k> =
    if Set.contains x s then
        failwithf "Item '%O' already exists" x
    else
        Set.add x s

let ofSeqUnique (items : seq<'a>) : Set<'a> =
    Seq.fold (flip addUnique) Set.empty items
