module FunWithFlags.FunDB.Utils

open System
open System.Collections.Generic
open System.Globalization
open Microsoft.FSharp.Reflection

type Void = private Void of unit

module Result =
    let isOk : Result<'a, 'e> -> bool = function
        | Ok _ -> true
        | Error _ -> false

    let isError : Result<'a, 'e> -> bool = function
        | Ok _ -> false
        | Error _ -> true

    let get : Result<'a, 'e> -> 'a = function
        | Ok v -> v
        | Error _ -> failwith "Result.get"

    let getError : Result<'a, 'e> -> 'e = function
        | Ok _ -> failwith "Result.getError"
        | Error v -> v

module Seq =
    let mapMaybe (f : 'a -> 'b option) (s : seq<'a>) : seq<'b> =
        seq { for i in s do
                  match f i with
                  | Some r -> yield r
                  | None -> ()
            }

    let mapiMaybe (f : int -> 'a -> 'b option) (s : seq<'a>) : seq<'b> =
        seq { let mutable n = 0
              for i in s do
                  match f n i with
                  | Some r -> yield r
                  | None -> ()
                  n <- n + 1
            }

    let first (s : seq<'a>) : 'a option = Seq.tryFind (fun x -> true) s

    let fold1 (func : 'a -> 'a -> 'a) (s : seq<'a>) : 'a =
        Seq.fold func (Seq.head s) (Seq.tail s)

    // FIXME: make those stop on first failure
    let traverseOption (func : 'a -> 'b option) (vals : seq<'a>) : seq<'b> option =
        let res = vals |> Seq.map func |> Seq.cache
        if Seq.forall Option.isSome res then
            Some (Seq.map Option.get res)
        else
            None

    let traverseResult (func : 'a -> Result<'b, 'e>) (vals : seq<'a>) : Result<seq<'b>, 'e> =
        let res = vals |> Seq.map func |> Seq.cache
        match res |> Seq.filter Result.isError |> Seq.first with
        | Some err -> Error <| Result.getError err
        | None -> Ok (Seq.map (Result.get) res)

module Map =
    let ofSeqUnique (items : seq<'k * 'v>) : Map<'k, 'v> =
        Seq.fold (fun m (k, v) -> if Map.containsKey k m then failwith (sprintf "Key '%s' already exists" (k.ToString ())) else Map.add k v m) Map.empty items

    let getWithDefault (k : 'k) (def : 'v) (m : Map<'k, 'v>) : 'v =
        match Map.tryFind k m with
        | Some v -> v
        | None -> def

    let unionUnique (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
        ofSeqUnique (Seq.append (Map.toSeq a) (Map.toSeq b))

    let union (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
        Map.ofSeq (Seq.append (Map.toSeq a) (Map.toSeq b))

    let difference (a : Map<'k, 'v>) (b : Map<'k, 'v>) : Map<'k, 'v> =
        Map.filter (fun k v -> not (Map.containsKey k b)) a

    let singleton (k : 'k) (v : 'v) : Map<'k, 'v> = Seq.singleton (k, v) |> Map.ofSeq

    let update (func : 'k -> 'a -> 'b option) (map : Map<'k, 'a>) : Map<'k, 'b> =
        map |> Map.toSeq |> Seq.mapMaybe (fun (k, a) -> Option.map (fun b -> (k, b)) <| func k a) |> Map.ofSeq

    let keys (map : Map<'k, 'v>) : seq<'k> =
        map |> Map.toSeq |> Seq.map fst

    let values (map : Map<'k, 'v>) : seq<'v> =
        map |> Map.toSeq |> Seq.map snd
    let keysSet (map : Map<'k, 'v>) : Set<'k> =
        map |> keys |> Set.ofSeq

    let mapWithKeys (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
        map |> Map.toSeq |> Seq.map (fun (k, a) -> func k a) |> Map.ofSeq
    
    let mapKeys (func : 'k1 -> 'k2) : Map<'k1, 'a> -> Map<'k2, 'a> = mapWithKeys (fun name v -> (func name, v))
    
    let traverseOption (func : 'k -> 'a -> 'b option) (vals : Map<'k, 'a>) : Map<'k, 'b> option =
        let res = vals |> Map.map func
        if Map.forall (fun key -> Option.isSome) res then
            Some (Map.map (fun key -> Option.get) res)
        else
            None

    let traverseResult (func : 'k -> 'a -> Result<'b, 'e>) (vals : Map<'k, 'a>) : Result<Map<'k, 'b>, 'e> =
        let res = vals |> Map.map func
        match res |> Map.toSeq |> Seq.filter (fun (_, r) -> Result.isError r) |> Seq.first with
        | Some (_, err) -> Error <| Result.getError err
        | None -> Ok <| Map.map (fun key -> Result.get) res

    let findWithDefault (k : 'k) (def : unit -> 'v) (vals : Map<'k, 'v>) : 'v =
        match Map.tryFind k vals with
        | None -> def ()
        | Some v -> v

    let reverse (map : Map<'k, 'v>): Map<'v, 'k> =
        map |> Map.toSeq |> Seq.map (fun (a, b) -> (b, a)) |> Map.ofSeq    

module Set =
    let toMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
        s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

    let getSingle (s : Set<'k>) : 'k = s |> Set.toSeq |> Seq.exactlyOne

    let ofSeqUnique (items : seq<'a>) : Set<'a> =
        Seq.fold (fun s x -> if Set.contains x s then failwith (sprintf "Item '%s' already exists" (x.ToString ())) else Set.add x s) Set.empty items

let tryInt (culture : CultureInfo) (str : string) : int option =
    match Int32.TryParse(str, NumberStyles.Integer ||| NumberStyles.AllowDecimalPoint, culture) with
    | (true, res) -> Some res
    | _ -> None

let tryIntInvariant : string -> int option = tryInt CultureInfo.InvariantCulture
let tryIntCurrent : string -> int option = tryInt CultureInfo.CurrentCulture

let tryDecimal (culture : CultureInfo) (str : string) : decimal option =
    match Decimal.TryParse(str, NumberStyles.Currency, culture) with
    | (true, res) -> Some res
    | _ -> None

let tryDecimalInvariant : string -> decimal option = tryDecimal CultureInfo.InvariantCulture
let tryDecimalCurrent : string -> decimal option = tryDecimal CultureInfo.CurrentCulture

let tryDateTime (culture : CultureInfo) (dateTimeStr : string) : DateTime option =
    match DateTime.TryParse(dateTimeStr, culture, DateTimeStyles.AssumeLocal ||| DateTimeStyles.AdjustToUniversal) with
    | (true, date) -> Some date
    | _ -> None

let tryDateTimeInvariant : string -> DateTime option = tryDateTime CultureInfo.InvariantCulture
let tryDateTimeCurrent : string -> DateTime option = tryDateTime CultureInfo.CurrentCulture

let tryDateTimeOffset (culture : CultureInfo) (dateTimeStr : string) : DateTimeOffset option =
    match DateTimeOffset.TryParse(dateTimeStr, culture, DateTimeStyles.AssumeLocal ||| DateTimeStyles.AdjustToUniversal) with
    | (true, date) -> Some date
    | _ -> None

let tryDateTimeOffsetInvariant : string -> DateTimeOffset option = tryDateTimeOffset CultureInfo.InvariantCulture
let tryDateTimeOffsetCurrent : string -> DateTimeOffset option = tryDateTimeOffset CultureInfo.CurrentCulture

let tryDate (culture : CultureInfo) (dateStr : string) : DateTimeOffset option =
    match tryDateTimeOffset culture dateStr with
    | Some date when date.Hour = 0 && date.Minute = 0 && date.Second = 0 && date.Millisecond = 0 -> Some date
    | _ -> None

let tryDateInvariant : string -> DateTimeOffset option = tryDate CultureInfo.InvariantCulture
let tryDateCurrent : string -> DateTimeOffset option = tryDate CultureInfo.CurrentCulture

let (|Regex|_|) (regex : string) (str : string) : (string list) option =
   let m = System.Text.RegularExpressions.Regex.Match(str, regex)
   if m.Success
   then Some <| List.tail [ for x in m.Groups -> x.Value ]
   else None

let dictUnique (items : IEnumerable<'k * 'v>) : IDictionary<'k, 'v> = items |> Map.ofSeqUnique |> Map.toSeq |> dict

let tryCast<'b> (value : obj) : 'b option =
    match value with
    | :? 'b as res -> Some res
    | _ -> None

let concatWithWhitespaces (strs : seq<string>) : string =
    let filterEmpty = function
        | "" -> None
        | str -> Some str
    strs |> Seq.mapMaybe filterEmpty |> String.concat " "

let tryBool (str : string) : bool option =
    match str.ToLower() with
    | "true" -> Some true
    | "t" -> Some true
    | "1" -> Some true
    | "yes" -> Some true
    | "y" -> Some true
    | "false" -> Some false
    | "f" -> Some false
    | "0" -> Some false
    | "no" -> Some false
    | "n" -> Some false
    | _ -> None

// Runtime cast for discriminated unions
let castUnion<'a> (value : obj) : 'a option =
    let cases = FSharpType.GetUnionCases(typeof<'a>)
    let gotType = value.GetType()
    let (gotCase, args) = FSharpValue.GetUnionFields(value, gotType)
    if gotCase.DeclaringType.GetGenericTypeDefinition() = typedefof<'a> then
        try
            Some (FSharpValue.MakeUnion(cases.[gotCase.Tag], args) :?> 'a)
        with
        | :? InvalidOperationException -> None
    else
        None