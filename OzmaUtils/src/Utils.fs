[<AutoOpen>]
module OzmaDB.OzmaUtils.Utils

open Printf
open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Threading
open System.Runtime.ExceptionServices
open Microsoft.FSharp.Reflection

type Void = private Void of unit

let inline isRefNull (x: 'a) = obj.ReferenceEquals(x, null)

let inline (|RefNull|_|) (value: 'a) =
    if isRefNull value then Some RefNull else None

let raisefWithInner (constr: (string * Exception) -> 'e) (inner: Exception) format =
    let thenRaise str = raise <| constr (str, inner)
    ksprintf thenRaise format

let raisef (constr: string -> 'e) format =
    let thenRaise str = raise <| constr str
    ksprintf thenRaise format

let inline reraise' (e: exn) =
    (ExceptionDispatchInfo.Capture e).Throw()
    failwith "Unreachable"

let private genericDictUnique (items: ('k * 'v) seq) =
    let d = Dictionary<'k, 'v>()

    for (k, v) in items do
        if not <| d.TryAdd(k, v) then
            failwithf "Key '%O' already exists" k

    ReadOnlyDictionary d

let readOnlyDictUnique (items: ('k * 'v) seq) : IReadOnlyDictionary<'k, 'v> =
    genericDictUnique items :> IReadOnlyDictionary<'k, 'v>

let dictUnique (items: ('k * 'v) seq) : IDictionary<'k, 'v> =
    genericDictUnique items :> IDictionary<'k, 'v>

let tryCast<'b> (value: obj) : 'b option =
    match value with
    | :? 'b as res -> Some res
    | _ -> None

// Runtime cast for discriminated unions.
let castUnion<'a> (value: obj) : 'a option =
    let cases = FSharpType.GetUnionCases(typeof<'a>)
    let gotType = value.GetType()
    let (gotCase, args) = FSharpValue.GetUnionFields(value, gotType)

    if gotCase.DeclaringType.GetGenericTypeDefinition() = typedefof<'a> then
        try
            Some(FSharpValue.MakeUnion(cases.[gotCase.Tag], args) :?> 'a)
        with :? InvalidOperationException ->
            None
    else
        None

let tryGetValue (dict: IReadOnlyDictionary<'K, 'V>) (key: 'K) : 'V option =
    let (success, v) = dict.TryGetValue(key)
    if success then Some v else None

let isUnionCase<'t> (objectType: Type) : bool =
    if not (FSharpType.IsUnion typeof<'t>) then
        failwith "Type is not a union"

    if (typeof<'t>).IsGenericType then
        not (isNull objectType.BaseType)
        && objectType.BaseType.IsGenericType
        && objectType.BaseType.GetGenericTypeDefinition() = typedefof<'t>
    else
        not (isNull objectType.BaseType) && objectType.BaseType = typeof<'t>

let inline unmaskableLock (k: 'Lock) (f: (unit -> unit) -> 'a) : 'a =
    let mutable lockWasTaken = false

    try
        Monitor.Enter(k, &lockWasTaken)

        let inline unmask () =
            if lockWasTaken then
                Monitor.Exit(k)
                lockWasTaken <- false

        f unmask
    finally
        if lockWasTaken then
            Monitor.Exit(k)
