[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Task

open System.Threading.Tasks
open FSharp.Control.Tasks.Affine

let inline map (f : 'r1 -> 'r2) (t : ^a) : Task<'r2> =
    task {
        let! ret = t
        return f ret
    }

let result (a : 'a) : Task<'a> =
    Task.FromResult a

let empty = Task.CompletedTask

let awaitSync (t : Task<'a>) : 'a =
    t.GetAwaiter().GetResult()

// https://github.com/fsharp/fslang-suggestions/issues/866
let inline deferAsync ([<InlineIfLambda>] deferredTask : unit -> Task) ([<InlineIfLambda>] f : unit -> Task<'a>) : Task<'a> =
    task {
        let! r =
            task {
                try
                    return! f ()
                with
                | e ->
                    do! deferredTask ()
                    return reraise' e
            }
        do! deferredTask ()
        return r
    }