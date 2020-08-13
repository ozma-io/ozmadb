module FunWithFlags.FunUtils.Array

open System.Threading.Tasks

let mapTask (f : 'a -> Task<'b>) (arr : 'a[]) : Task<'b[]> =
    Task.map Seq.toArray (Seq.mapTask f arr)