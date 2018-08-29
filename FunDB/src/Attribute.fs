namespace FunWithFlags.FunDB.Attribute

open System
open System.Collections
open System.Collections.Generic

// Basically JSON without nulls.
type AttributeMap (map : IDictionary<string, Attribute>) as this =
    let resolvePath xs extract = Array.toList xs |> this.RunResolvePath |> Option.map extract
    let resolvePathWithDefault xs extract def =
        match resolvePath xs extract with
            | Some(x) -> x
            | None -> def

    let tryFind x =
        if map.ContainsKey x then
            Some(map.[x])
        else
            None

    new() = new AttributeMap(dict Seq.empty)

    interface IEnumerable with
        member this.GetEnumerator () = (map :> IEnumerable).GetEnumerator ()

    member this.GetEnumerator () = map.GetEnumerator ()

    interface IEnumerable<KeyValuePair<string, Attribute>> with
        member this.GetEnumerator () = this.GetEnumerator ()

    member this.Count = map.Count
    member this.IsReadOnly = true

    member this.Add x = raise (new InvalidOperationException("AttributeMap is read only"))
    member this.Clear () = raise (new InvalidOperationException("AttributeMap is read only"))
    member this.Contains x = map.Contains x
    member this.CopyTo (arr, i) = map.CopyTo (arr, i)
    member this.Remove (x : KeyValuePair<string, Attribute>) = raise (new InvalidOperationException("AttributeMap is read only"))

    interface ICollection<KeyValuePair<string, Attribute>> with
        member this.Count = this.Count
        member this.IsReadOnly = this.IsReadOnly

        member this.Add x = this.Add x
        member this.Clear () = this.Clear ()
        member this.Contains x = this.Contains x
        member this.CopyTo (arr, i) = this.CopyTo (arr, i)
        member this.Remove (x : KeyValuePair<string, Attribute>) = this.Remove x

    member this.Item
        with get index = map.[index]
        and set index (value : Attribute) = raise (new InvalidOperationException("AttributeMap is read only"))

    member this.Keys = map.Keys
    member this.Values = map.Values

    member this.Add (k, v) = raise (new InvalidOperationException("AttributeMap is read only"))
    member this.ContainsKey k = map.ContainsKey k
    member this.Remove (k : string) = raise (new InvalidOperationException("AttributeMap is read only"))
    member this.TryGetValue (k, v : Attribute byref) = map.TryGetValue (k, ref v)

    interface IDictionary<string, Attribute> with
        member this.Item
            with get index = this.Item index
            and set index value =
                this.Item(index) <- value
        member this.Keys = this.Keys
        member this.Values = this.Values
        member this.Add (k, v) = this.Add (k, v)
        member this.ContainsKey k = this.ContainsKey k
        member this.Remove (k : string) = this.Remove k
        member this.TryGetValue (k, v : Attribute byref) = this.TryGetValue (k, ref v)

    member private this.RunResolvePath = function
        | [] -> None
        | [x] -> tryFind x
        | x :: xs ->
            match tryFind x with
                | None -> None
                | Some(v) -> v.GetAssoc().RunResolvePath(xs)

    member this.GetBoolOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetBool ())
    member this.GetFloatOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetFloat ())
    member this.GetIntOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetInt ())
    member this.GetStringOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetString ())
    member this.GetListOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetList ())
    member this.GetAssocOption ([<ParamArray>] xs) = resolvePath xs (fun x -> x.GetAssoc ())

    member this.GetBoolWithDefault (def : bool, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetBool ()) def
    member this.GetFloatWithDefault (def : float, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetFloat ()) def
    member this.GetIntWithDefault (def : int, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetInt ()) def
    member this.GetStringWithDefault (def : string, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetString ()) def
    member this.GetListWithDefault (def : Attribute array, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetList ()) def
    member this.GetAssocWithDefault (def : AttributeMap, [<ParamArray>] xs) = resolvePathWithDefault xs (fun x -> x.GetAssoc ()) def

    static member Merge (a : AttributeMap) (b : AttributeMap) : AttributeMap =
        let insertOne oldMap = function
            | KeyValue(k, v) ->
                match Map.tryFind k oldMap with
                    | None -> Map.add k v oldMap
                    | Some(oldv) -> Map.add k (Attribute.Merge oldv v) oldMap
        new AttributeMap(Seq.fold insertOne (Seq.fold insertOne Map.empty a) b |> Map.toSeq |> dict)

and Attribute =
    | ABool of bool
    | AFloat of float
    | AInt of int
    | AString of string
    | AList of Attribute array
    | AAssoc of AttributeMap
with
    member this.GetBool () =
        match this with
            | ABool(b) -> b
            | _ -> invalidOp "GetBool"
    member this.GetFloat () =
        match this with
            | AFloat(f) -> f
            | _ -> invalidOp "GetFloat"
    member this.GetInt () =
        match this with
            | AInt(i) -> i
            | _ -> invalidOp "GetInt"
    member this.GetString () =
        match this with
            | AString(s) -> s
            | _ -> invalidOp "GetString"
    member this.GetList () =
        match this with
            | AList(l) -> l
            | _ -> invalidOp "GetList"
    member this.GetAssoc () : AttributeMap =
        match this with
            | AAssoc(a) -> a
            | _ -> invalidOp "GetAssoc"

    static member Merge (a : Attribute) (b : Attribute) : Attribute =
        match (a, b) with
            | (AList(a), AList(b)) -> AList(Array.append a b)
            | (AAssoc(a), AAssoc(b)) -> AAssoc(AttributeMap.Merge a b)
            | _ -> b
