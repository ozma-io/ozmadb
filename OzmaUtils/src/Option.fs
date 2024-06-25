[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Option

open System.Threading.Tasks
open FSharpPlus

let getOrFailWith (errorFunc: unit -> string) : 'a option -> 'a =
    function
    | Some r -> r
    | None -> failwith (errorFunc ())

let toSeq (a: 'a option) : 'a seq =
    match a with
    | Some r -> Seq.singleton r
    | None -> Seq.empty

let mapTask (f: 'a -> Task<'b>) (opt: 'a option) : Task<'b option> =
    match opt with
    | None -> Task.result None
    | Some a -> Task.map Some (f a)

let addWith (f: 'a -> 'a -> 'a) (a: 'a) (b: 'a option) : 'a =
    match b with
    | Some bv -> f a bv
    | None -> a

let unionWith (f: 'a -> 'a -> 'a) (a: 'a option) (b: 'a option) : 'a option =
    match (a, b) with
    | (None, None) -> None
    | (Some v, None)
    | (None, Some v) -> Some v
    | (Some v1, Some v2) -> Some(f v1 v2)

let unionUnique (a: 'a option) (b: 'a option) : 'a option =
    unionWith (fun v1 v2 -> failwith "Value exists in both") a b
