[<AutoOpen>]
module FunWithFlags.FunUtils.TopLevel

open Printf
open System
open System.Collections.Generic
open System.Runtime.ExceptionServices
open Microsoft.FSharp.Reflection

open FunWithFlags.FunUtils

module Task = Task
module Null = Null
module Option = Option
module Result = Result
module Seq = Seq
module Set = Set
module Map = Map
module Array = Array
module String = String
module Parsing = Parsing
module Collections = Collections
module IO = IO
module Expr = Expr
module POSIXPath = POSIXPath
module Hash = Hash

type Void = private Void of unit
type Exception = System.Exception
type StringComparable<'a> = String.StringComparable<'a>

let inline isRefNull (x : 'a) = obj.ReferenceEquals(x, null)

let inline curry f a b = f (a, b)
let inline uncurry f (a, b) = f a b

let dictUnique (items : IEnumerable<'k * 'v>) : IDictionary<'k, 'v> = items |> Map.ofSeqUnique |> Map.toSeq |> dict

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
        sprintf "%s: %s" e.Message (exceptionString e.InnerException)