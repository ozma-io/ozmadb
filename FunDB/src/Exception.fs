module FunWithFlags.FunDB.Exception

open System
open Printf
open Newtonsoft.Json.Linq
open System.Runtime.Serialization

// Exceptions which can be marked as "user exceptions" -- messages in them can be safely
// sent back to client.
type IUserException =
    abstract member IsUserException : bool
    abstract member UserData : JToken option

// For most exceptions we build a final error string concatenating all nested exception messages together,
// separated with `:`. However, for some exceptions we want to just use the current message,
// without adding messages from inner exceptions. An example is JavaScript exceptions,
// which we'd like to format with a JavaScript stack trace.
type ICustomFormatException =
    abstract member MessageContainsInnerError : bool
    abstract member ShortMessage : string

let isUserException (e : exn) =
    match box e with
    | null -> true
    | :? IUserException as e -> e.IsUserException
    | _ -> false

let userExceptionData (e : exn) =
    match box e with
    | :? IUserException as e -> e.UserData
    | _ -> None

type UserException (message : string, innerException : Exception, isUserException : bool, userData : JToken option) =
    inherit Exception(message, innerException)

    new (message : string, innerException : Exception, isUserException : bool) =
        UserException (message, innerException, isUserException, userExceptionData innerException)

    new (message : string, innerException : Exception) =
        UserException (message, innerException, isUserException innerException, userExceptionData innerException)

    new (message : string) = UserException (message, null, true, None)

    member this.IsUserException = isUserException
    member this.UserData = userData

    interface IUserException with
        member this.IsUserException = isUserException
        member this.UserData = userData

let rec fullUserMessage (e : exn) : string =
    let containsInner =
        match box e with
        | :? ICustomFormatException as ce -> ce.MessageContainsInnerError
        | _ -> false
    if isNull e.InnerException || containsInner then
        e.Message
    else
        let inner = fullUserMessage e.InnerException
        if e.Message = "" then
            inner
        else if inner = "" then
            e.Message
        else
            sprintf "%s: %s" e.Message inner

let inline raisefUserWithInner<'a, 'b, 'e when 'e :> UserException> (constr : (string * Exception * bool) -> 'e) (inner : Exception) : StringFormat<'a, 'b> -> 'a =
    kprintf <| fun str ->
        raise  <| constr (str, inner, true)

type ILoggableResponse =
    abstract ShouldLog : bool

type IErrorDetails =
    inherit ILoggableResponse

    abstract LogMessage : string
    [<DataMember>] abstract Message : string
    abstract HTTPResponseCode : int