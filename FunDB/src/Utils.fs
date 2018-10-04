module FunWithFlags.FunDB.Utils

open System
open System.Collections.Generic
open System.Globalization

type Void = private Void of unit

module Seq =
    let mapMaybe (f : 'a -> 'b option) (s : 'a seq) : 'b seq =
        seq { for i in s do
                  match f i with
                      | Some r -> yield r
                      | None -> ()
            }

    let mapiMaybe (f : int -> 'a -> 'b option) (s : 'a seq) : 'b seq =
        seq { let mutable n = 0
              for i in s do
                  match f n i with
                      | Some r -> yield r
                      | None -> ()
                  n <- n + 1
            }

    let first (s : 'a seq) : 'a option = Seq.tryFind (fun x -> true) s

    let fold1 (func : 'a -> 'a -> 'a) (s : 'a seq) : 'a =
        Seq.fold func (Seq.head s) (Seq.tail s)

module Map =
    let ofSeqUnique (items : ('k * 'v) seq) : Map<'k, 'v> =
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

    let keysSet (map : Map<'k, 'v>) : Set<'k> =
        map |> Map.toSeq |> Seq.map fst |> Set.ofSeq

    let mapWithKeys (func : 'k1 -> 'a -> ('k2 * 'b)) (map : Map<'k1, 'a>) : Map<'k2, 'b> =
        map |> Map.toSeq |> Seq.map (fun (k, a) -> func k a) |> Map.ofSeq

module Set =
    let toMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
        s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

    let getSingle (s : Set<'k>) : 'k = s |> Set.toSeq |> Seq.exactlyOne

    let ofSeqUnique (items : 'a seq) : Set<'a> =
        Seq.fold (fun s x -> if Set.contains x s then failwith (sprintf "Item '%s' already exists" (x.ToString ())) else Set.add x s) Set.empty items

let tryInt (culture : CultureInfo) (str : string) : int option =
    match Int32.TryParse(str, NumberStyles.Integer, culture) with
        | (true, res) -> Some res
        | _ -> None

let tryIntInvariant : string -> int option = tryInt CultureInfo.InvariantCulture
let tryIntCurrent : string -> int option = tryInt CultureInfo.CurrentCulture

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

let dictUnique (items : ('k * 'v) seq) : IDictionary<'k, 'v> = items |> Map.ofSeqUnique |> Map.toSeq |> dict

let tryCast<'b> (value : obj) : 'b option =
    match value with
        | :? 'b as res -> Some res
        | _ -> None

let concatWithWhitespaces (strs : string seq) : string =
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
