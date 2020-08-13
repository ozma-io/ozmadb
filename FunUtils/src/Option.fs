module FunWithFlags.FunUtils.Option

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

let getOrFailWith (errorFunc : unit -> string) : 'a option -> 'a = function
    | Some r -> r
    | None -> failwith (errorFunc ())

let toSeq : 'a option -> 'a seq = function
    | Some r -> Seq.singleton r
    | None -> Seq.empty

let mapTask (f : 'a -> Task<'b>) (opt : 'a option) : Task<'b option> =
    task {
        match opt with
        | None -> return None
        | Some a -> return! Task.map Some (f a)
    }

let ofNull (obj : 'a) : 'a option =
    if isNull obj then
        None
    else
        Some obj

let toNull : 'a option -> 'a = function
    | None -> null
    | Some x -> x

let unionWith (f : 'a -> 'a -> 'a) (a : 'a option) (b : 'a option) : 'a option =
    match (a, b) with
    | (None, None) -> None
    | (Some v, None)
    | (None, Some v) -> Some v
    | (Some v1, Some v2) -> Some (f v1 v2)

let unionUnique (a : 'a option) (b : 'a option) : 'a option =
    unionWith (fun v1 v2 -> failwith "Value exists in both") a b
