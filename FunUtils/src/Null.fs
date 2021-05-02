[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Null

let defaultValue (defaultValue : 'a) : 'a -> 'a = function
    | null -> defaultValue
    | a -> a

let defaultWith (defaultThunk : unit -> 'a) : 'a -> 'a = function
    | null -> defaultThunk ()
    | a -> a