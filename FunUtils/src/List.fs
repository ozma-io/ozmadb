[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.List

let snoc : 'a list -> 'a * 'a list = function
    | head :: tail -> (head, tail)
    | [] -> failwith "snoc: empty list"

let trySnoc : 'a list -> ('a * 'a list) option = function
    | head :: tail -> Some (head, tail)
    | [] -> None