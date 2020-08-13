module FunWithFlags.FunUtils.Task

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

let bind (f : 'a -> Task<'b>) (a : Task<'a>) : Task<'b> =
    task {
        let! r = a
        return! f r
    }

let result (a : 'a) : Task<'a> =
    Task.FromResult a

let map (f : 'a -> 'b) (a : Task<'a>) : Task<'b> =
    task {
        let! r = a
        return f r
    }

let map2Sync (f : 'a -> 'b -> 'c) (a : Task<'a>) (b : Task<'b>) : Task<'c> =
    task {
        let! r1 = a
        let! r2 = b
        return f r1 r2
    }

let awaitSync (t : Task<'a>) : 'a =
    t.GetAwaiter().GetResult()