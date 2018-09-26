namespace FunWithFlags.FunDB.Attribute

open System
open System.Collections
open System.Collections.Generic

type AttributeMap<'e> (map : IDictionary<string, Attribute>) as this =
    let getOption name extract = tryFind name |> Option.map extract
    let getOptionWithDefault name extract def =
        match getOption xs extract with
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

    member this.GetBoolOption name = getOption name (fun x -> x.GetBool ())
    member this.GetFloatOption name = getOption name (fun x -> x.GetFloat ())
    member this.GetIntOption name = getOption name (fun x -> x.GetInt ())
    member this.GetStringOption name = getOption name (fun x -> x.GetString ())

    member this.GetBoolWithDefault (def, name) = getOptionWithDefault name (fun x -> x.GetBool ()) def
    member this.GetFloatWithDefault (def, name) = getOptionWithDefault name (fun x -> x.GetFloat ()) def
    member this.GetIntWithDefault (def, name) = getOptionWithDefault name (fun x -> x.GetInt ()) def
    member this.GetStringWithDefault (def, name) = getOptionWithDefault name (fun x -> x.GetString ()) def

    static member Merge (a : AttributeMap) (b : AttributeMap) : AttributeMap =
        let insertOne oldMap = function
            | KeyValue(k, v) ->
                match Map.tryFind k oldMap with
                    | None -> Map.add k v oldMap
                    | Some(oldv) -> Map.add k v oldMap
        new AttributeMap(Seq.fold insertOne (Seq.fold insertOne Map.empty a) b |> Map.toSeq |> dict)

and Attribute<'e> =
    | ABool of bool
    | AFloat of float
    | AInt of int
    | AString of string
    | AExpr of 'e
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
