module FunWithFlags.FunDB.HTTP.LongRunning

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.IO
open StackExchange
open FSharp.Control.Tasks.Affine
open Newtonsoft.Json
open Microsoft.Extensions.Logging

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.Serialization.Utils

type JobId = string

type RemoteJobSettings =
    { Multiplexer : Redis.IConnectionMultiplexer
      ClientId : string
      IdleTimeout : TimeSpan
    }

type RemoteJobRef =
    { Settings : RemoteJobSettings
      Id : JobId
    }

let private remoteJobName (remote : RemoteJobRef) = sprintf "job:%s:%O" remote.Settings.ClientId remote.Id

let private remoteJobDataName (remote : RemoteJobRef) = sprintf "job:%s:%O:data" remote.Settings.ClientId remote.Id

// TODO: make it private after we fix JSON serialization for the private F# types.
[<SerializeAsObject("result")>]
type SavedJobResult<'a> =
    | [<CaseKey("reply")>] SJReply of Header : 'a
    | [<CaseKey("cancelled")>] SJCancelled
    | [<CaseKey("internalError")>] SJInternalError

type RemoteJobResult<'a> =
    | RJNotFound
    | RJPending
    | RJReply of Header : 'a * Data : ReadOnlyMemory<byte>
    | RJCanceled
    | RJInternalError

let private parseRemoteResult (remote : RemoteJobRef) (connection : Redis.IDatabase) (rawResult : Redis.RedisValue) (cancellationToken : CancellationToken) =
    task {
        let jobDataName = remoteJobDataName remote
        let resultString = Redis.RedisValue.op_Implicit(rawResult) : string
        let result = JsonConvert.DeserializeObject<SavedJobResult<'a>>(resultString)
        match result with
        | SJCancelled -> return RJCanceled
        | SJInternalError -> return RJInternalError
        | SJReply header ->
            let! rawResultData = connection.StringGetAsync(jobDataName)
            if rawResultData.IsNull then
                return RJNotFound
            else
                let resultData = Redis.RedisValue.op_Implicit(rawResultData) : ReadOnlyMemory<byte>
                return RJReply (header, resultData)
    }

let private waitRemoteJob<'a> (remote : RemoteJobRef) (delay : TimeSpan) (cancellationToken : CancellationToken) : Task<RemoteJobResult<'a>> =
    task {
        let jobName = remoteJobName remote
        let jobKey = Redis.RedisKey(jobName)
        let jobChannel = Redis.RedisChannel.op_Implicit(jobName)

        let subscr = remote.Settings.Multiplexer.GetSubscriber()
        let! queue = subscr.SubscribeAsync(jobChannel)
        try
            let conn = remote.Settings.Multiplexer.GetDatabase()
            let! rawResult = conn.StringGetSetExpiryAsync(jobKey, delay + remote.Settings.IdleTimeout)
            if rawResult.IsNull then
                return RJNotFound
            else if not rawResult.IsNullOrEmpty then
                return! parseRemoteResult remote conn rawResult cancellationToken
            else
                use newSource = new CancellationTokenSource()
                newSource.CancelAfter(delay)
                let cancelToken = cancellationToken.Register(fun () -> newSource.Cancel())
                try
                    try
                        let! gotResult = queue.ReadAsync(newSource.Token)
                        let! rawResult = conn.StringGetAsync(jobKey)
                        return! parseRemoteResult remote conn rawResult cancellationToken
                    with
                    | :? OperationCanceledException ->
                        cancellationToken.ThrowIfCancellationRequested ()
                        return RJPending
                finally
                    ignore <| cancelToken.Unregister()
        finally
            ignore <| queue.UnsubscribeAsync ()
    }

type JobDataWriter =
    | JOAsync of (Stream -> CancellationToken -> Task)
    | JOSync of (Stream -> CancellationToken -> unit)

let getJobDataBytes (rmsManager : RecyclableMemoryStreamManager) (writer : JobDataWriter) (cancellationToken : CancellationToken) : Task<ReadOnlyMemory<byte>> =
    task {
        use memoryStream = rmsManager.GetStream("getJobDataBytes")
        match writer with
        | JOAsync writer ->
            do! writer memoryStream cancellationToken
        | JOSync writer ->
            writer memoryStream cancellationToken
        return IO.getMemoryStreamMemory memoryStream
    }

let streamJobData (rmsManager : RecyclableMemoryStreamManager) (writer : JobDataWriter) (stream : Stream) (setSize : int64 option -> unit) (cancellationToken : CancellationToken) : Task =
    task {
        match writer with
        | JOAsync writer ->
            setSize None
            do! writer stream cancellationToken
        | JOSync writer ->
            use memoryStream = rmsManager.GetStream("streamJobData")
            writer memoryStream cancellationToken
            setSize (Some memoryStream.Position)
            ignore <| memoryStream.Seek(0L, SeekOrigin.Begin)
            do! memoryStream.CopyToAsync(stream, cancellationToken)
    }

type private LocalJobStatus<'a> =
    | JSNoJob
    | JSLocal of Task<'a * JobDataWriter>
    | JSConsumedLocal
    | JSRemote

let private cancelRemoteSource (cancelSource : CancellationTokenSource) =
    if not cancelSource.IsCancellationRequested then
        try
            cancelSource.Cancel ()
        with
        | :? ObjectDisposedException -> ()

type LongRunningJob<'a> (
            rmsManager : RecyclableMemoryStreamManager,
            logger : ILogger<LongRunningJob<'a>>,
            cancellationToken : CancellationToken
        ) =
    let cancelSource = new CancellationTokenSource ()
    let mutable status = JSNoJob

    let remoteCancel () = cancelRemoteSource cancelSource

    let cancelRegistration = cancellationToken.UnsafeRegister(
        (fun that -> cancelRemoteSource (that :?> CancellationTokenSource)),
        cancelSource
    )

    let runCleanupJob (remote : RemoteJobRef) (expireTime : TimeSpan) : Task =
        let jobKey = Redis.RedisKey(remoteJobName remote)

        let rec loop (expireTime : TimeSpan) =
            unitTask {
                do! Task.Delay(expireTime, cancelSource.Token)
                let conn = remote.Settings.Multiplexer.GetDatabase()
                let! newExpireTime = conn.KeyTimeToLiveAsync(jobKey)
                if not newExpireTime.HasValue then
                    logger.LogInformation("Cancelling remote job {jobId}", remote.Id)
                    remoteCancel ()
                else
                    do! loop newExpireTime.Value
            }
        loop expireTime

    let runSubmitJob (job : Task<'a * JobDataWriter>) (remote : RemoteJobRef) =
        unitTask {
            try
                let conn = remote.Settings.Multiplexer.GetDatabase()
                let! result =
                    task {
                        try
                            let! (result, resultWriter) = job
                            let! resultData = getJobDataBytes rmsManager resultWriter cancelSource.Token
                            logger.LogInformation("Submitting the result of the job {jobId}", remote.Id)
                            let! successData = conn.StringSetAsync(
                                Redis.RedisKey(remoteJobDataName remote),
                                Redis.RedisValue.op_Implicit(resultData),
                                remote.Settings.IdleTimeout
                            )
                            return SJReply result
                        with
                        | _ when cancelSource.IsCancellationRequested -> return SJCancelled
                        | e ->
                            logger.LogError(e, "An unhandled exception has occurred while executing the long-running job {jobId}.", remote.Id)
                            return SJInternalError
                    }

                let jobName = remoteJobName remote
                let encodedResult = JsonConvert.SerializeObject(result)
                let! success = conn.StringSetAsync(
                    Redis.RedisKey(jobName),
                    Redis.RedisValue(encodedResult),
                    remote.Settings.IdleTimeout
                )
                let! successPublish = conn.PublishAsync(
                    Redis.RedisChannel.op_Implicit(jobName),
                    "finished"
                )
                ()
            finally
                cancelSource.Cancel ()
                cancelRegistration.Dispose ()
                cancelSource.Dispose ()
        }

    static member WaitRemote (remote : RemoteJobRef) (delay : TimeSpan) (cancellationToken : CancellationToken) =
        waitRemoteJob remote delay cancellationToken

    member this.Start (runJob : CancellationToken -> Task<'a * JobDataWriter>) =
        match status with
        | JSNoJob ->
            status <- JSLocal (runJob cancelSource.Token)
        | _ ->
            raisef InvalidOperationException "The job has already been started"

    member this.LocalWait (delay : TimeSpan) : Task<('a * JobDataWriter) option> =
        task {
            match status with
            | JSLocal job ->
                let! jobOrTimer = Task.WhenAny(job, Task.Delay(delay, cancelSource.Token))
                if job.IsCompleted then
                    let! ret = job
                    status <- JSConsumedLocal
                    return Some ret
                else
                    return None
            | _ -> return raisef InvalidOperationException "Job is not running locally"
        }

    member this.ConvertToRemote (settings : RemoteJobSettings) : Task<RemoteJobRef> =
        task {
            match status with
            | JSLocal job ->
                let conn = settings.Multiplexer.GetDatabase()
                // The Redis library does not support cancellation tokens (???)
                // https://github.com/StackExchange/StackExchange.Redis/issues/1039
                let jobId = string <| Guid.NewGuid()
                let remote =
                    { Settings = settings
                      Id = jobId
                    }
                let jobName = remoteJobName remote
                let! success = conn.StringSetAsync(Redis.RedisKey(jobName), Redis.RedisValue(""), settings.IdleTimeout)
                logger.LogInformation("Converted the job {jobId} to remote", jobId)
                ignore <| runSubmitJob job remote
                ignore <| runCleanupJob remote settings.IdleTimeout
                status <- JSRemote
                return remote
            | _ -> return raisef InvalidOperationException "Job is not running locally"
        }

    member this.Cancel () =
        match status with
        | JSNoJob
        | JSLocal _ -> cancelSource.Cancel ()
        | JSConsumedLocal -> ()
        | JSRemote -> remoteCancel ()

    member this.CancelAfter (timeout : TimeSpan) =
        match status with
        | JSNoJob -> raisef InvalidOperationException "Job is not running"
        | JSLocal job -> cancelSource.CancelAfter timeout
        | JSConsumedLocal -> ()
        | JSRemote ->
            if not cancelSource.IsCancellationRequested then
                try
                    cancelSource.CancelAfter timeout
                with
                | :? ObjectDisposedException -> ()

    interface IDisposable with
        member this.Dispose () =
            // If the job has not yet been started, we dispose of the cancellation source.
            // Otherwise, the background task owns it.
            match status with
            | JSNoJob ->
                cancelRegistration.Dispose ()
                cancelSource.Dispose ()
            | JSLocal job ->
                cancelSource.Cancel ()
                cancelRegistration.Dispose ()
                cancelSource.Dispose ()
            | JSConsumedLocal ->
                cancelRegistration.Dispose ()
                cancelSource.Dispose ()
            | JSRemote -> ()
