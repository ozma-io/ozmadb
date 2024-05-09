[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.List

let snoc : 'a list -> 'a * 'a list = function
    | head :: tail -> (head, tail)
    | [] -> failwith "snoc: empty list"

let trySnoc : 'a list -> ('a * 'a list) option = function
    | head :: tail -> Some (head, tail)
    | [] -> None

let rec exceptLast : 'a list -> 'a list = function
    | [head] -> []
    | head :: tail -> head :: exceptLast tail
    | [] -> failwith "exceptLast: empty list"
