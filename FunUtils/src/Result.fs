[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Result

let isOk : Result<'a, 'e> -> bool = function
    | Ok _ -> true
    | Error _ -> false

let isError : Result<'a, 'e> -> bool = function
    | Ok _ -> false
    | Error _ -> true

let getOption : Result<'a, 'e> -> 'a option = function
    | Ok v -> Some v
    | Error _ -> None

let getErrorOption : Result<'a, 'e> -> 'e option = function
    | Ok _ -> None
    | Error v -> Some v

let getError : Result<'a, 'e> -> 'e = function
    | Ok _ -> failwith "Result.getError"
    | Error v -> v

let getValue (f : 'e -> 'a) : Result<'a, 'e> -> 'a = function
    | Ok a -> a
    | Error e -> f e