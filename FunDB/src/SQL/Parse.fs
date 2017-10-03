module internal FunWithFlags.FunDB.SQL.Parse

open FunWithFlags.FunDB.SQL.Value

let parseBool (str : string) =
    match str.ToLower() with
        | "true" -> Some(true)
        | "false" -> Some(false)
        | _ -> None

// Return Nones instead of throwing exceptions
let parseValue (valType : ValueType) (str : string) =
    if str = null
    then Some(VNull)
    else
        try
            match valType with
                | VTInt -> Some(VInt(int str))
                | VTFloat -> Some(VFloat(float str))
                | VTString -> Some(VString(str))
                | VTBool -> str |> parseBool |> Option.map VBool
        with
            _ -> None
