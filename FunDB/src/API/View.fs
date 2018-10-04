module FunWithFlags.FunDB.API.View

open System.Linq
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json
open Suave
open Suave.Filters
open Suave.Operators

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Lexer
open FunWithFlags.FunDB.FunQL.Parser
open FunWithFlags.FunDB.FunQL.View
open FunWithFlags.FunDB.FunQL.Query
open FunWithFlags.FunDB.FunQL.Result
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.Context

let private parseExprArgument (fieldExprType : FieldExprType) (str : string) : FieldValue option =
    let decodeArray constrFunc convertFunc =
            let res = str.Split(',') |> Array.map convertFunc
            if Array.forall Option.isSome res then
                Some <| constrFunc (Array.map Option.get res)
            else
                None
    match fieldExprType with
        // FIXME: breaks strings with commas!
        | FETArray SFTString -> failwith "Not supported yet"
        | FETArray SFTInt -> decodeArray FIntArray tryIntInvariant
        | FETArray SFTBool -> decodeArray FBoolArray tryBool
        | FETArray SFTDateTime -> decodeArray FDateTimeArray tryDateTimeOffsetInvariant
        | FETArray SFTDate -> decodeArray FDateArray tryDateInvariant
        | FETScalar SFTString -> Some <| FString str
        | FETScalar SFTInt -> Option.map FInt <| tryIntInvariant str
        | FETScalar SFTBool -> Option.map FBool <| tryBool str
        | FETScalar SFTDateTime -> Option.map FDateTime <| tryDateTimeOffsetInvariant str
        | FETScalar SFTDate -> Option.map FDate <| tryDateInvariant str

let private convertArgument (fieldType : ParsedFieldType) (str : string) : FieldValue option =
    match fieldType with
        | FTType feType -> parseExprArgument feType str
        | FTReference (entityRef, where) -> Option.map FInt <| tryIntInvariant str
        | FTEnum values -> Some <| FString str

let viewsApi (rctx : RequestContext) : WebPart =
    fun ctx -> async {
        let runSelect (view : ResolvedViewExpr) : WebPart =
            fun ctx -> async {
                let rawArgs = ctx.request.query |> Map.ofList
                let findArgument name (fieldType : ParsedFieldType) =
                    match Map.tryFind (name.ToString()) rawArgs with
                        | Some (Some argStr) ->
                            match convertArgument fieldType argStr with
                                | None -> failwith <| sprintf "Cannot convert argument %O to type %O" name fieldType
                                | Some arg -> arg
                        | _ -> failwith <| sprintf "Argument not found: %O" name
                let maybeArgs =
                    try
                        Ok <| Map.map findArgument view.arguments
                    with
                        | Failure msg -> Result.Error msg
                match maybeArgs with
                    | Result.Error msg -> return! RequestErrors.BAD_REQUEST msg ctx
                     | Ok args ->
                        try
                            let result = getResultViewExpr rctx.Connection.Query rctx.Layout view args
                            return! jsonResponse result ctx
                        with
                            | ViewExecutionError msg -> return! ServerErrors.INTERNAL_ERROR msg ctx
            }

        let updatingViewApi (view : ResolvedViewExpr) : WebPart =
            if rctx.UserName <> rootUserName then
                RequestErrors.FORBIDDEN ""
            else if true then
                Writers.setHeader "Allow" "GET" >=> RequestErrors.METHOD_NOT_ALLOWED ""
            else
                choose
                    [ POST >=> Successful.OK "ahaha"
                      PUT >=> Successful.OK "oh wow"
                      DELETE >=> Successful.OK "omg"
                    ]

        let viewApi (view : ResolvedViewExpr) : WebPart =
            choose
                [ GET >=> runSelect view
                  updatingViewApi view
                ]

        let withRawView (rawView : string) : WebPart =
            match parse tokenizeFunQL viewExpr rawView with
                | Result.Error msg -> RequestErrors.BAD_REQUEST <| sprintf "Cannot parse FunQL view expression: %s" msg
                | Ok rawExpr ->
                    let maybeExpr =
                        try
                            Ok <| resolveViewExpr rctx.Layout rawExpr
                        with
                            | ViewError err -> Result.Error err
                    match maybeExpr with
                        | Result.Error err -> RequestErrors.UNPROCESSABLE_ENTITY <| sprintf "Cannot resolve FunQL view expression: %s" err
                        | Ok expr -> viewApi expr

        let anonymousView : WebPart =
            request <| fun req ->
                match req.queryParam "__query" with
                    | Choice1Of2 rawView -> withRawView rawView
                    | Choice2Of2 _ -> RequestErrors.BAD_REQUEST "Query not specified"
        
        let namedView (name : string) : WebPart =
            fun ctx ->
                async {
                    let! maybeUv = rctx.Connection.System.UserViews.Where(fun uv -> uv.Name = name).FirstOrDefaultAsync()
                    match maybeUv : UserView with
                        | null -> return! RequestErrors.NOT_FOUND "" ctx
                        | uv -> return! withRawView uv.Query ctx
                }

        return!
            choose
                [ path "/layout" >=> GET >=> jsonResponse rctx.AllowedDatabase
                  path "/views/anonymous" >=> anonymousView
                  pathScan "/views/by_name/%s" (fun viewName -> namedView viewName)
                ] ctx
    }