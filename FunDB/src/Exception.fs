module FunWithFlags.FunDB.Exception

open System
open Printf

// Exceptions which can be marked as "user exceptions" -- messages in them can be safely
// sent back to client.
type UserException (message : string, innerException : Exception, isUserException : bool) =
    inherit Exception(message, innerException)

    new (message : string, innerException : Exception) =
        let isUserException = UserException.IsThatUserException innerException
        UserException (message, innerException, isUserException)

    new (message : string) = UserException (message, null, true)

    static member internal IsThatUserException (e : Exception) =
        match e with
        | null -> true
        | :? UserException as e -> e.IsUserException
        | _ -> false

    member this.IsUserException = isUserException

let isUserException e = UserException.IsThatUserException e

let inline raisefUserWithInner<'a, 'b, 'e when 'e :> UserException> (constr : (string * Exception * bool) -> 'e) (inner : Exception) : StringFormat<'a, 'b> -> 'a =
    kprintf <| fun str ->
        raise  <| constr (str, inner, true)
