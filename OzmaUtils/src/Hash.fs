[<RequireQualifiedAccess>]
module OzmaDB.OzmaUtils.Hash

open System.Text
open System.Security.Cryptography

let sha1OfBytes (bytes : byte[]) : byte[] =
    use hasher = SHA1.Create()
    hasher.ComputeHash bytes

let sha1OfString (str : string) : byte[] =
    let bytes = Encoding.UTF8.GetBytes str
    sha1OfBytes bytes