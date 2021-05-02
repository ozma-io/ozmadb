namespace FunWithFlags.FunUtils

open System
open System.Collections.Generic

type ObjectMap = private ObjectMap of IDictionary<Type, obj>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module ObjectMap =
    let empty : ObjectMap = ObjectMap (dict Seq.empty)

    let size (ObjectMap map) : int = map.Count

    let isEmpty (map : ObjectMap) : bool = size map = 0

    let ofSeq (objs : obj seq) : ObjectMap =
        objs |> Seq.map (fun o -> (o.GetType(), o)) |> dict |> ObjectMap

    let singleton (obj : obj) : ObjectMap =
        ofSeq (Seq.singleton obj)

    let toSeq (ObjectMap map) : obj seq =
        map |> Seq.map (fun (KeyValue(otyp, o)) -> o)

    let filter (f : Type -> obj -> bool) (ObjectMap map) : ObjectMap =
        map |> Seq.filter (fun (KeyValue(otyp, o)) -> f otyp o) |> Seq.map (fun (KeyValue(otyp, o)) -> (otyp, o)) |> dict |> ObjectMap

    let add (obj : obj) (ObjectMap map) : ObjectMap =
        Seq.append (Seq.map (fun (KeyValue(otyp, o)) -> (otyp, o)) map) (Seq.singleton (obj.GetType(), obj)) |> dict |> ObjectMap

    let delete (typ : Type) (ObjectMap map) : ObjectMap =
        map |> Seq.filter (fun (KeyValue(otyp, o)) -> typ <> otyp) |> Seq.map (fun (KeyValue(otyp, o)) -> (otyp, o)) |> dict |> ObjectMap

    let deleteType<'a> (map : ObjectMap) : ObjectMap = delete typeof<'a> map

    let tryFind (typ : Type) (ObjectMap map) : obj option =
        match map.TryGetValue typ with
        | (true, obj) -> Some obj
        | _ -> None

    let tryFindType<'a> (ObjectMap map : ObjectMap) : 'a option =
        match map.TryGetValue typeof<'a> with
        | (true, obj) -> Some (obj :?> 'a)
        | _ -> None

    let find (typ : Type) (map : ObjectMap) : obj =
        match tryFind typ map with
        | Some obj -> obj
        | None -> raise <| KeyNotFoundException()

    let findType<'a> (map : ObjectMap) : 'a =
        find typeof<'a> map :?> 'a

    let contains (typ : Type) (ObjectMap map) : bool =
        map.ContainsKey typ

    let containsType<'a> (map : ObjectMap) : bool =
        contains typeof<'a> map
