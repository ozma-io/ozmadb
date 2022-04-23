namespace FunWithFlags.FunUtils

open System
open System.Collections.Generic
open System.Collections.Immutable

[<Struct>]
type ObjectMap = private ObjectMap of HashMap<Type, obj>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module ObjectMap =
    let empty : ObjectMap = ObjectMap HashMap.empty

    let size (ObjectMap map) : int = HashMap.size map

    let isEmpty (ObjectMap map) : bool = HashMap.isEmpty map

    let ofSeq (objs : obj seq) : ObjectMap =
        objs |> Seq.map (fun o -> KeyValuePair(o.GetType(), o)) |> ImmutableDictionary.CreateRange |> ObjectMap

    let singleton (obj : obj) : ObjectMap =
        HashMap.singleton (obj.GetType()) obj |> ObjectMap

    let toSeq (ObjectMap map) : obj seq =
        map |> Seq.map (fun (KeyValue(otyp, o)) -> o)

    let filter (f : Type -> obj -> bool) (ObjectMap map) : ObjectMap =
        map |> Seq.filter (fun (KeyValue(otyp, o)) -> f otyp o) |> ImmutableDictionary.CreateRange |> ObjectMap

    let add (obj : obj) (ObjectMap map) : ObjectMap =
        HashMap.add (obj.GetType()) obj map |> ObjectMap

    let remove (typ : Type) (ObjectMap map) : ObjectMap =
        HashMap.remove typ map |> ObjectMap

    let removeType<'a> (map : ObjectMap) : ObjectMap = remove typeof<'a> map

    let tryFind (typ : Type) (ObjectMap map) : obj option =
        match map.TryGetValue typ with
        | (true, obj) -> Some obj
        | _ -> None

    let tryFindType<'a> (ObjectMap map) : 'a option =
        match map.TryGetValue typeof<'a> with
        | (true, obj) -> Some (obj :?> 'a)
        | _ -> None

    let find (typ : Type) (ObjectMap map) : obj =
        match map.TryGetValue typ with
        | (true, obj) -> obj
        | _ -> raise <| KeyNotFoundException()

    let findType<'a> (map : ObjectMap) : 'a =
        find typeof<'a> map :?> 'a

    let contains (typ : Type) (ObjectMap map) : bool =
        map.ContainsKey typ

    let containsType<'a> (map : ObjectMap) : bool =
        contains typeof<'a> map
