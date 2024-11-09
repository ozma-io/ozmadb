[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Task

open System
open System.Threading
open System.Threading.Tasks

let inline map (f: 'r1 -> 'r2) (t: ^a) : Task<'r2> =
    task {
        let! ret = t
        return f ret
    }

let result (a: 'a) : Task<'a> = Task.FromResult a

let waitFor (t: Task) =
    t.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing)

let empty = Task.CompletedTask

let awaitSync (t: Task<'a>) : 'a = t.GetAwaiter().GetResult()

// https://github.com/fsharp/fslang-suggestions/issues/866
let inline toDisposable ([<InlineIfLambda>] deferredTask: unit -> Task) : IAsyncDisposable =
    { new IAsyncDisposable with
        member this.DisposeAsync() : ValueTask = deferredTask () |> ValueTask }

// https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/task-expressions#limitations-of-tasks-regarding-tailcalls
[<Struct>]
type LoopResult<'arg, 'r> =
    | NextLoop of Argument: 'arg
    | StopLoop of Result: 'r

let inline loop (arg: 'arg) ([<InlineIfLambda>] f: 'arg -> Task<LoopResult<'arg, 'r>>) : Task<'r> =
    task {
        let mutable result = Unchecked.defaultof<'r>
        let mutable arg = arg
        let mutable continueLoop = true

        while continueLoop do
            match! f arg with
            | NextLoop nextArg -> arg <- nextArg
            | StopLoop loopResult ->
                result <- loopResult
                continueLoop <- false

        return result
    }

// Kinda like an explicit and tracking TaskScheduler.
type ICustomTaskScheduler =
    abstract member Post: (unit -> Task<'a>) -> Task<'a>
    abstract member Post: (unit -> Task) -> Task
    abstract member WaitAll: CancellationToken -> Task

type NoCustomTaskScheduler() =
    member this.Post(f: unit -> Task<'a>) : Task<'a> = f ()

    member this.Post(f: unit -> Task) : Task = f ()

    member this.WaitAll(cancellationToken: CancellationToken) : Task = Task.CompletedTask

    member this.CheckedWaitAll(cancellationToken: CancellationToken) : Task = Task.CompletedTask

    interface ICustomTaskScheduler with
        member this.Post(f: unit -> Task<'a>) = this.Post f
        member this.Post(f: unit -> Task) = this.Post f
        member this.WaitAll cancellationToken = this.WaitAll cancellationToken

type LastTaskRef() =
    let mutable lastTask = Task.CompletedTask
    let id = Random.Shared.Next()

    do printfn "Creating a scheduled task ref, %d" id

    member this.Exchange(task: Task) : Task =
        printfn "Adding a scheduled task, %d" id
        Interlocked.Exchange(&lastTask, task)

    member this.WaitForLastTask(cancellationToken: CancellationToken) : Task =
        loop ()
        <| fun () ->
            task {
                let currLastTask = lastTask
                printfn "Waiting for a scheduled task, %d" id
                do! waitFor <| currLastTask.WaitAsync(cancellationToken)
                cancellationToken.ThrowIfCancellationRequested()

                if lastTask = currLastTask then
                    return StopLoop()
                else
                    return NextLoop()
            }
        :> Task

type TrackingTaskScheduler() =
    let lastTask = LastTaskRef()

    member this.Post<'t when 't :> Task>(f: unit -> 't) : 't =
        let thisTask = f ()
        let nextTaskSource = TaskCompletionSource()
        let prevTask = lastTask.Exchange nextTaskSource.Task

        ignore
        <| task {
            do! waitFor prevTask
            do! waitFor thisTask
            nextTaskSource.SetResult()
        }

        thisTask

    member this.WaitAll(cancellationToken: CancellationToken) : Task =
        lastTask.WaitForLastTask cancellationToken

    interface ICustomTaskScheduler with
        member this.Post(f: unit -> Task<'a>) = this.Post f
        member this.Post(f: unit -> Task) = this.Post f
        member this.WaitAll cancellationToken = this.WaitAll cancellationToken

type SerializingTrackingTaskScheduler() =
    let lastTask = LastTaskRef()

    member this.Post(f: unit -> Task<'a>) : Task<'a> =
        let nextTaskSource = TaskCompletionSource()
        let prevTask = lastTask.Exchange nextTaskSource.Task

        task {
            do! waitFor prevTask

            try
                return! f ()
            finally
                nextTaskSource.SetResult()
        }

    member this.Post(f: unit -> Task) : Task =
        let nextTaskSource = TaskCompletionSource()
        let prevTask = lastTask.Exchange nextTaskSource.Task

        task {
            do! waitFor prevTask

            try
                do! f ()
            finally
                nextTaskSource.SetResult()
        }

    member this.WaitAll(cancellationToken: CancellationToken) : Task =
        lastTask.WaitForLastTask cancellationToken

    interface ICustomTaskScheduler with
        member this.Post(f: unit -> Task<'a>) = this.Post f
        member this.Post(f: unit -> Task) = this.Post f
        member this.WaitAll cancellationToken = this.WaitAll cancellationToken
