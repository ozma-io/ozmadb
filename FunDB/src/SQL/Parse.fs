module internal FunWithFlags.FunDB.SQL.Parse

open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.AST

let parseBool (str : string) =
    match str.ToLower() with
        | "true" -> Some(true)
        | "false" -> Some(false)
        | _ -> None

let parseYesNo (str : string) =
    match str.ToLower() with
        | "yes" -> Some(true)
        | "no" -> Some(false)
        | _ -> None

// Return Nones instead of throwing exceptions
let parseSimpleValue (valType : ValueType) (str : string) =
    if str = null
    then Some(VNull)
    else
        try
            match valType with
                | VTInt -> Some(VInt(int str))
                | VTFloat -> Some(VFloat(float str))
                | VTString -> Some(VString(str))
                | VTBool -> str |> parseBool |> Option.map VBool
                | VTObject -> failwith "Objects are not meant to be parsed"
        with
            _ -> None

let parseValueType (str : string) =
    match str.ToLower() with
        | "integer" -> Some(VTInt)
        | "double precision" -> Some(VTFloat)
        | "text" -> Some(VTString)
        | "boolean" -> Some(VTBool)
        | _ -> None

let parseConstraintType (str : string) =
    match str.ToUpper() with
        | "UNIQUE" -> Some(CTUnique)
        | "PRIMARY KEY" -> Some(CTPrimaryKey)
        | "FOREIGN KEY" -> Some(CTForeignKey)
        | _ -> None
