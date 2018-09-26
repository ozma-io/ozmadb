module FunWithFlags.FunDB.Utils

open System
open System.Globalization
open System.Text.RegularExpressions

type Void = private Void of unit

let mapGetWithDefault (k : 'k) (def : 'v) (m : Map<'k, 'v>) : 'v =
    match Map.tryFind k m with
        | Some(v) -> v
        | None -> def

let setToMap (f : 'k -> 'v) (s : Set<'k>) : Map<'k, 'v> =
    s |> Set.toSeq |> Seq.map (fun k -> (k, f k)) |> Map.ofSeq

let setSingleItem (s : Set<'k>) : 'v = s |> Set.toSeq |> Seq.exactlyOne

let seqMapMaybe (f : 'a -> 'b option) (s : 'a seq) : 'b seq =
    seq { for i in s do
              match f i with
                  | Some(r) -> yield r
                  | None -> ()
        }

let seqFirst (s : 'a seq) : 'a option = Seq.tryFind (fun x -> true) s

let seqFold1 (func : 'a -> 'a -> 'a) (s : 'a seq) : 'a =
    Seq.fold func (Seq.head s) (Seq.tail s)

let mapOfSeqUnique (items : ('k * 'v) seq) : Map<'k, 'v> =
    Seq.fold (fun m (k, v) -> if Map.containsKey k m then failwith (sprintf "Key '%s' already exists" (k.ToString ())) else Map.add k v m) Map.empty items

let setOfSeqUnique (items : 'a seq) : Set<'a> =
    Seq.fold (fun s x -> if Set.contains x s then failwith (sprintf "Item '%s' already exists" (x.ToString ())) else Set.add x s) Set.empty items

let mapUnionUnique (a : Map<'k, 'v>) (b : Map<'k, 'v>) =
    mapOfSeqUnique (Seq.append (Map.toSeq a) (Map.toSeq b))

let mapUnion (a : Map<'k, 'v>) (b : Map<'k, 'v>) =
    Map.ofSeq (Seq.append (Map.toSeq a) (Map.toSeq b))

let mapDifference (a : Map<'k, 'v1>) (b : Map<'k, 'v2>) =
    Map.filter (fun k v -> not (Map.containsKey k b)) a

let mapSingleton (k : 'k) (v : 'v) : Map<'k, 'v> =
    Map.ofSeq (seq { yield (k, v) })

let mapKeys (a : Map<'k, 'v>) : Set<'k> =
    Map.toSeq >> Seq.map fst >> Set.ofSeq

let tryInt (culture : CultureInfo) (str : string) : int option =
    match Int32.TryParse(str, NumberStyles.Integer, culture) with
        | (true, res) -> Some(res)
        | _ -> None

let tryIntInvariant : string -> int option = tryInt CultureInfo.InvariantCulture
let tryIntCurrent : string -> int option = tryInt CultureInfo.CurrentCulture

let tryDateTime (culture : CultureInfo) (dateStr : string) : DateTime option =
    match DateTime.TryParse(dateStr, culture, DateTimeStyles.AssumeLocal ||| DateTimeStyles.AdjustToUniversal) with
        | (true, date) -> Some(date)
        | _ -> None

let tryDateTimeInvariant : string -> DateTime option = tryDateTime CultureInfo.InvariantCulture
let tryDateTimeCurrent : string -> DateTime option = tryDateTime CultureInfo.CurrentCulture

let (|Regex|_|) (regex : string) (str : string) : string list =
   let m = System.Text.RegularExpressions.Regex.Match(str, regex)
   if m.Success
   then Some(List.tail [ for x in m.Groups -> x.Value ])
   else None

let dictUnique (items : ('k * 'v) seq) : IDictionary<'k, 'v> = items |> mapOfSeqUnique |> dict

let tryCast<'b> : 'a -> 'b option = function
    | :? 'T as res -> Some(res)
    | _ -> None

let concatWithWhitespaces (strs : string seq) : string =
    let filterEmpty = function
        | "" = None
        | str -> Some(str)
    strs |> seqMapMaybe filterEmpty |> String.concat " "

let tryBool : (str : string) : bool option =
    match str.ToLower() with
        | "true" -> Some true
        | "t" -> Some true
        | "yes" -> Some true
        | "y" -> Some true
        | "false" -> Some false
        | "f" -> Some false
        | "no" -> Some false
        | "n" _> Some false
        | _ -> None
