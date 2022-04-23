namespace FunWithFlags.FunUtils

open System.Collections.Immutable

type HashSet<'a> = ImmutableHashSet<'a>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module HashSet =
    let empty<'a> : HashSet<'a> = ImmutableHashSet.Create ()

    let size (map : HashSet<'a>) : int = map.Count

    let isEmpty (map : HashSet<'a>) : bool = map.IsEmpty

    let ofSeq (objs : 'a seq) : HashSet<'a> = ImmutableHashSet.CreateRange objs

    let ofSeqUnique (items : 'a seq) : ImmutableHashSet<'a> =
        let builder = ImmutableHashSet.CreateBuilder<'a>()
        for item in items do
            if not <| builder.Add(item) then
                failwithf "Item '%O' already exists" item
        ImmutableHashSet.ToImmutableHashSet builder

    let singleton (item : 'a) : HashSet<'a> = ImmutableHashSet.Create<'a>([|item|])

    let toSeq (map : HashSet<'a>) : 'a seq = map

    let filter (f : 'a -> bool) (map : HashSet<'a>) : HashSet<'a> =
        map |> Seq.filter f |> ImmutableHashSet.CreateRange

    let add (item : 'a) (map : HashSet<'a>) : HashSet<'a> =
        map.Add(item)

    let delete (item : 'a) (map : HashSet<'a>) : HashSet<'a> =
        map.Remove(item)

    let contains (key : 'a) (map : HashSet<'a>) : bool =
        map.Contains key
