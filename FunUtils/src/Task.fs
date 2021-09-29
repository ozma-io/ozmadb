[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Task

open System.Threading.Tasks
open FSharp.Control.Tasks.NonAffine
open System.Collections.Generic

// https://devblogs.microsoft.com/pfxteam/building-async-coordination-primitives-part-5-asyncsemaphore/
type AsyncSemaphore (initialCount : int) =
    let mutable currentCount = initialCount
    let waiters = Queue()
    
    member this.WaitAsync () : Task =
        lock waiters <| fun () ->
            if currentCount > 0 then
                currentCount <- currentCount - 1
                Task.CompletedTask
            else
                let waiter = TaskCompletionSource()
                waiters.Enqueue(waiter)
                waiter.Task :> Task

    member this.Release () =
        let maybeToRelease =
            Operators.lock waiters <| fun () ->
                if waiters.Count > 0 then
                    Some <| waiters.Dequeue()
                else
                    currentCount <- currentCount + 1
                    None
        match maybeToRelease with
        | None -> ()
        | Some toRelease -> toRelease.SetResult(null)

let result (a : 'a) : Task<'a> =
    Task.FromResult a

let awaitSync (t : Task<'a>) : 'a =
    t.GetAwaiter().GetResult()

let inline lock (k : AsyncSemaphore) (f : unit -> Task<'a>) : Task<'a> =
    task {
        do! k.WaitAsync()
        try
            return! f ()
        finally
            k.Release()
    }

let inline unmaskableLock (k : AsyncSemaphore) (f : (unit -> unit) -> Task<'a>) : Task<'a> =
    task {
        let mutable lockWasTaken = true
        do! k.WaitAsync()
        try
            let inline unmask () =
                if lockWasTaken then
                    k.Release()
                    lockWasTaken <- false
            return! f unmask
        finally
            if lockWasTaken then
                k.Release()
    }
