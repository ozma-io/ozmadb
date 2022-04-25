[<AutoOpen>]
module FunWithFlags.FunUtils.Utils

open Printf
open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Threading
open System.Runtime.ExceptionServices
open Microsoft.FSharp.Reflection

type Void = private Void of unit
type Exception = System.Exception

let inline isRefNull (x : 'a) = obj.ReferenceEquals(x, null)

let inline raisefWithInner (constr : (string * Exception) -> 'e) (inner : Exception) : StringFormat<'a, 'b> -> 'a =
    kprintf <| fun str ->
        raise  <| constr (str, inner)

let inline raisef (constr : string -> 'e) : StringFormat<'a, 'b> -> 'a =
    let thenRaise str =
        raise  <| constr str
    kprintf thenRaise

let inline reraise' (e : exn) =
    (ExceptionDispatchInfo.Capture e).Throw ()
    failwith "Unreachable"

let private genericDictUnique (items : ('k * 'v) seq) =
    let d = Dictionary<'k, 'v>()
    for (k, v) in items do
        if not <| d.TryAdd(k, v) then
            failwithf "Key '%O' already exists" k
    ReadOnlyDictionary d

let readOnlyDictUnique (items : ('k * 'v) seq) : IReadOnlyDictionary<'k, 'v> =
    genericDictUnique items :> IReadOnlyDictionary<'k, 'v>

let dictUnique (items : ('k * 'v) seq) : IDictionary<'k, 'v> =
    genericDictUnique items :> IDictionary<'k, 'v>

let tryCast<'b> (value : obj) : 'b option =
    match value with
    | :? 'b as res -> Some res
    | _ -> None

// Runtime cast for discriminated unions.
let castUnion<'a> (value : obj) : 'a option =
    let cases = FSharpType.GetUnionCases(typeof<'a>)
    let gotType = value.GetType()
    let (gotCase, args) = FSharpValue.GetUnionFields(value, gotType)
    if gotCase.DeclaringType.GetGenericTypeDefinition() = typedefof<'a> then
        try
            Some (FSharpValue.MakeUnion(cases.[gotCase.Tag], args) :?> 'a)
        with
        | :? InvalidOperationException -> None
    else
        None

let tryGetValue (dict: IReadOnlyDictionary<'K, 'V>) (key: 'K) : 'V option =
    let (success, v) = dict.TryGetValue(key)
    if success then Some v else None

let isUnionCase<'t> (objectType : Type) : bool =
    if not (FSharpType.IsUnion typeof<'t>) then
        failwith "Type is not a union"

    if (typeof<'t>).IsGenericType then
        not (isNull objectType.BaseType) && objectType.BaseType.IsGenericType && objectType.BaseType.GetGenericTypeDefinition() = typedefof<'t>
    else
        not (isNull objectType.BaseType) && objectType.BaseType = typeof<'t>

let rec exceptionString (e : exn) : string =
    if isNull e.InnerException then
        e.Message
    else
        let inner = exceptionString e.InnerException
        if e.Message = "" then
            inner
        else if inner = "" then
            e.Message
        else
            sprintf "%s: %s" e.Message inner

let inline unmaskableLock (k : 'Lock) (f : (unit -> unit) -> 'a) : 'a =
    let mutable lockWasTaken = false
    try
        Monitor.Enter(k, ref lockWasTaken)
        let inline unmask () =
            if lockWasTaken then
                Monitor.Exit(k)
                lockWasTaken <- false
        f unmask
    finally
        if lockWasTaken then
            Monitor.Exit(k)