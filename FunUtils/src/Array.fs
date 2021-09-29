[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Array

open System.Threading.Tasks
open FSharpPlus

let mapTask (f : 'a -> Task<'b>) (arr : 'a[]) : Task<'b[]> =
    Task.map Seq.toArray (Seq.mapTask f arr)

let rec exceptLast (arr : 'a array) : 'a array =
    let len = Array.length arr
    if len = 0 then
        failwith "exceptLast: empty array"
    Array.take (len - 1) arr