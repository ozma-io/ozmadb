module FunWithFlags.FunDB.Exception

open System
open Printf

// Exceptions which can be marked as "user exceptions" -- messages in them can be safely
// sent back to client.
type IUserException =
    abstract member IsUserException : bool

// For most exceptions we build a final error string concatenating all nested exception messages together,
// separated with `:`. However, for some exceptions we want to just use the current message,
// without adding messages from inner exceptions. An example is JavaScript exceptions,
// which we'd like to format with a JavaScript stack trace.
type ICustomFormatException =
    abstract member MessageContainsInnerError : bool
    abstract member ShortMessage : string

type UserException (message : string, innerException : Exception, isUserException : bool) =
    inherit Exception(message, innerException)

    new (message : string, innerException : Exception) =
        let isUserException = UserException.IsThatUserException innerException
        UserException (message, innerException, isUserException)

    new (message : string) = UserException (message, null, true)

    static member internal IsThatUserException (e : Exception) =
        match box e with
        | null -> true
        | :? IUserException as e -> e.IsUserException
        | _ -> false

    member this.IsUserException = isUserException

    interface IUserException with
        member this.IsUserException = isUserException

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

let isUserException e = UserException.IsThatUserException e

let inline raisefUserWithInner<'a, 'b, 'e when 'e :> UserException> (constr : (string * Exception * bool) -> 'e) (inner : Exception) : StringFormat<'a, 'b> -> 'a =
    kprintf <| fun str ->
        raise  <| constr (str, inner, true)
