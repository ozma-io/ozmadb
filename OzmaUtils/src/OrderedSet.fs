namespace OzmaDB.OzmaUtils

open System
open System.Collections
open System.Collections.Generic

type OrderedSet<'a> when 'a : comparison =
    private { Order : 'a[]; Keys : Set<'a> }

    override this.ToString () = this.Order.ToString ()

    member this.Count = this.Order.Length

    member this.Contains key = Set.contains key this.Keys

    member this.GetEnumerator () = (this.Order :> IEnumerable<'a>).GetEnumerator ()

    member this.CopyTo (arr : 'a[], arrayIndex : int) =
        this.Order.CopyTo(arr, arrayIndex)
    
    interface IEnumerable with
        member this.GetEnumerator () = this.GetEnumerator ()

    interface IEnumerable<'a> with
        member this.GetEnumerator () = this.GetEnumerator ()

    interface ICollection<'a> with
        member this.Count = this.Count
        member this.IsReadOnly = true
        member this.Add _ = raise <| NotSupportedException ()
        member this.Clear () = raise <| NotSupportedException ()
        member this.Contains pair = this.Contains pair
        member this.CopyTo (arr, arrayIndex) = this.CopyTo (arr, arrayIndex)
        member this.Remove _ = raise <| NotSupportedException ()

    interface IReadOnlyCollection<'a> with
        member this.Count = this.Count

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module OrderedSet =
    let empty : OrderedSet<'a> = { Order = [||]; Keys = Set.empty }

    let size (map : OrderedSet<'a>) : int = Set.count map.Keys

    let isEmpty (map : OrderedSet<'a>) : bool = Set.isEmpty map.Keys

    let inline private ofSeqGeneric (onDuplicate : 'a -> unit) (objs : 'a seq) : OrderedSet<'a> =
        let order = List()
        let mutable keys = Set.empty
        for i in objs do
            if Set.contains i keys then
                onDuplicate i
            else
                order.Add(i)
                keys <- Set.add i keys
        { Order = order.ToArray(); Keys = keys }

    let ofSeq (objs : 'a seq) = ofSeqGeneric (fun _ -> ()) objs

    let ofSeqUnique (objs : 'a seq) = ofSeqGeneric (fun item -> failwithf "Item '%O' already exists" item) objs

    let singleton (item : 'a) : OrderedSet<'a> =
        { Order = [|item|]
          Keys = Set.singleton item
        }

    let toSeq (map : OrderedSet<'a>) : 'a seq = map.Order

    let toArray (map : OrderedSet<'a>) : 'a[] = Array.copy map.Order

    let toSet (map : OrderedSet<'a>) : Set<'a> = map.Keys

    let filter (f : 'a -> bool) (map : OrderedSet<'a>) : OrderedSet<'a> =
        let mutable newKeys = map.Keys

        let checkKey k =
            if f k then
                true
            else
                newKeys <- Set.remove k newKeys
                false
        let newOrder = Array.filter checkKey map.Order

        { Order = newOrder
          Keys = newKeys
        }

    let add (item : 'a) (map : OrderedSet<'a>) : OrderedSet<'a> =
        if Set.contains item map.Keys then
            map
        else
            { Order = Array.add item map.Order
              Keys = Set.add item map.Keys
            }

    let remove (item : 'a) (map : OrderedSet<'a>) : OrderedSet<'a> =
        filter (fun citem -> citem = item) map

    let contains (key : 'a) (map : OrderedSet<'a>) : bool =
        Set.contains key map.Keys

    let map (f : 'a -> 'b) (map : OrderedSet<'a>) : OrderedSet<'b> = map |> toSeq |> Seq.map f |> ofSeq

    let difference (map1 : OrderedSet<'a>) (map2 : OrderedSet<'a>) : OrderedSet<'a> =
        filter (fun citem -> contains citem map2) map2