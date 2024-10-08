[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Task

open System
open System.Threading.Tasks

let inline map (f: 'r1 -> 'r2) (t: ^a) : Task<'r2> =
    task {
        let! ret = t
        return f ret
    }

let result (a: 'a) : Task<'a> = Task.FromResult a

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
        let mutable continueLoop = false

        while continueLoop do
            match! f arg with
            | NextLoop nextArg -> arg <- nextArg
            | StopLoop loopResult ->
                result <- loopResult
                continueLoop <- false

        return result
    }
