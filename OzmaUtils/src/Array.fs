[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Array

open System.Threading.Tasks
open FSharpPlus

let add (item : 'a) (arr : 'a[]) : 'a[] =
    Array.insertAt arr.Length item arr

let mapTask (f : 'a -> Task<'b>) (arr : 'a[]) : Task<'b[]> =
    Task.map Seq.toArray (Seq.mapTask f arr)

let rec exceptLast (arr : 'a array) : 'a array =
    let len = Array.length arr
    if len = 0 then
        failwith "exceptLast: empty array"
    Array.take (len - 1) arr

let filteri (f : int -> 'a -> bool) (arr : 'a[]) : 'a[] =
    let mutable currI = 0
    let check value =
        let ret = f currI value
        currI <- currI + 1
        ret
    Array.filter check arr