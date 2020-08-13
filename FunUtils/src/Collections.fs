module FunWithFlags.FunUtils.Collections

open System
open System.Collections.Generic

type private ObjectItem = OIDelayed of (unit -> obj) | OIObject of obj

type ObjectSet () =
    let objects = Dictionary<Type, ObjectItem>()

    member this.Add (o : 'a) =
        if o.GetType() <> typeof<'a> then
            failwith "Object type should be exactly the type parameter of Add"
        objects.Add(typeof<'a>, OIObject o)

    member this.Add (f : unit -> 'a) =
        let constr () =
            let o = f ()
            if o.GetType() <> typeof<'a> then
                failwith "Object type should be exactly the type parameter of Add"
            o :> obj
        objects.Add(typeof<'a>, OIDelayed constr)

    member this.Contains (t : Type) = objects.ContainsKey t
    member this.Contains<'a> () = objects.ContainsKey (typeof<'a>)

    member this.TryGet (t : Type) : obj option =
        match objects.TryGetValue(t) with
        | (false, _) -> None
        | (true, OIObject o) -> Some o
        | (true, OIDelayed f) ->
            let o = f ()
            objects.[t] <- OIObject o
            Some o

    member this.Get (t : Type) : obj =
        this.TryGet t |> Option.get

    member this.Get<'a> () : 'a = this.Get(typeof<'a>) :?> 'a
    member this.TryGet<'a> () : 'a option = this.TryGet(typeof<'a>) |> Option.map (fun f -> f :?> 'a)

    member this.GetOrCreate (f : unit -> 'a) =
        let t = typeof<'a>
        match this.TryGet(t) with
        | Some o -> o :?> 'a
        | None ->
            let o = f ()
            objects.[t] <- OIObject o
            o