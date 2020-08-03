module FunWithFlags.FunDB.SQL.Parsing

open System.Text
open FSharp.Text.Lexing

open FunWithFlags.FunUtils.Utils

let escapedString (startPos : int) (lexbuf: LexBuffer<_>) : string =
    let strBuilder = StringBuilder lexbuf.LexemeLength
    let getCode (toNumber : char -> int option) (numBase : int) =
        let rec go (minLen : int) (maxLen : int) (pos : int) (curr : int) =
            let returnNow () =
                if minLen > 0 then
                    failwith <| sprintf "Invalid value escape sequence at position %i" pos
                (curr, pos)
            if pos >= lexbuf.LexemeLength - 1 || maxLen = 0 then
                returnNow ()
            else
                match toNumber (lexbuf.LexemeChar pos) with
                | Some n -> go (minLen - 1) (maxLen - 1) (pos + 1) (curr * numBase + n)
                | None -> returnNow ()
        go

    let rec parseChar i =
        if i < lexbuf.LexemeLength - 1 then
            match lexbuf.LexemeChar i with
            | '\\' ->
                if i = lexbuf.LexemeLength - 2 then
                    failwith "Unexpected escape at the end of string"
                match lexbuf.LexemeChar (i + 1) with
                | '\\' ->
                    ignore <| strBuilder.Append('\\')
                    parseChar (i + 2)
                | 'b' ->
                    ignore <| strBuilder.Append('\x08')
                    parseChar (i + 2)
                | 'f' ->
                    ignore <| strBuilder.Append('\x0c')
                    parseChar (i + 2)
                | 'n' ->
                    ignore <| strBuilder.Append('\n')
                    parseChar (i + 2)
                | 'r' ->
                    ignore <| strBuilder.Append('\r')
                    parseChar (i + 2)
                | 't' ->
                    ignore <| strBuilder.Append('\t')
                    parseChar (i + 2)
                | n when n >= '0' && n <= '9' ->
                    let (ord, nextI) = getCode NumBases.octChar 8 0 2 (i + 2) (n |> NumBases.octChar |> Option.get)
                    ignore <| strBuilder.Append(char ord)
                    parseChar nextI
                | 'x' ->
                    let (ord, nextI) = getCode NumBases.hexChar 16 1 2 (i + 2) 0
                    ignore <| strBuilder.Append(char ord)
                    parseChar nextI
                | 'u' ->
                    let (ord, nextI) = getCode NumBases.hexChar 16 4 4 (i + 2) 0
                    ignore <| strBuilder.Append(char ord)
                    parseChar nextI
                | 'U' ->
                    let (ord, nextI) = getCode NumBases.hexChar 16 8 8 (i + 2) 0
                    ignore <| strBuilder.Append(char ord)
                    parseChar nextI
                | nextC ->
                    ignore <| strBuilder.Append(nextC)
                    parseChar (i + 2)
            | '\'' -> failwith <| sprintf "Unexpected quote character at position %i" i
            | c ->
                ignore <| strBuilder.Append(c)
                parseChar (i + 1)

    parseChar (startPos + 1) // Skip quotes
    strBuilder.ToString()

let escapedId (startPos : int) (lexbuf: LexBuffer<_>) : string =
    let strBuilder = StringBuilder lexbuf.LexemeLength

    let rec parseChar i =
        if i < lexbuf.LexemeLength - 1 then
            match lexbuf.LexemeChar i with
            | '"' ->
                if i = lexbuf.LexemeLength - 2 then
                    failwith "Unexpected quote at the end of identifier"
                match lexbuf.LexemeChar(i + 1) with
                | '"' ->
                    ignore <| strBuilder.Append('"')
                    parseChar (i + 2)
                | nextC -> failwith <| sprintf "Unexpected quote at position %i" i
            | c ->
                ignore <| strBuilder.Append(c)
                parseChar (i + 1)

    parseChar (startPos + 1) // Skip quotes
    strBuilder.ToString()

let sqlString (startPos : int) (lexbuf: LexBuffer<_>) : string =
    let strBuilder = StringBuilder lexbuf.LexemeLength

    let rec parseChar i =
        if i < lexbuf.LexemeLength - 1 then
            match lexbuf.LexemeChar i with
            | '\'' ->
                if i = lexbuf.LexemeLength - 2 then
                    failwith "Unexpected quote at the end of string"
                match lexbuf.LexemeChar(i + 1) with
                | '\'' ->
                    ignore <| strBuilder.Append('\'')
                    parseChar (i + 2)
                | nextC -> failwith <| sprintf "Unexpected quote at position %i" i
            | c ->
                ignore <| strBuilder.Append(c)
                parseChar (i + 1)

    parseChar (startPos + 1) // Skip quotes
    strBuilder.ToString()

let arrayString (startPos : int) (lexbuf: LexBuffer<_>) : string =
    let strBuilder = StringBuilder lexbuf.LexemeLength

    let rec parseChar i =
        if i < lexbuf.LexemeLength - 1 then
            match lexbuf.LexemeChar i with
            | '\"' ->  failwith <| sprintf "Unexpected quote character at position %i" i
            | '\\' ->
                if i = lexbuf.LexemeLength - 2 then
                    failwith "Unexpected escape at the end of string"
                let nextC = lexbuf.LexemeChar(i + 1)
                ignore <| strBuilder.Append(nextC)
                parseChar (i + 2)
            | c ->
                ignore <| strBuilder.Append(c)
                parseChar (i + 1)

    parseChar (startPos + 1) // Skip quotes
    strBuilder.ToString()