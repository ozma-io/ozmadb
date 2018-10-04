module FunWithFlags.FunDB.API.View

open System.Linq
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json
open Suave
open Suave.Filters
open Suave.Operators

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.FunQL.Query

[<NoComparison>]
type ViewEntriesGetResponse =
    { info : MergedViewInfo
      result : ExecutedViewExpr
    }

let viewsApi (rctx : RequestContext) : WebPart =
    fun ctx -> async {
       (*let selectView (view : UserViewRef) : WebPart =
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
                    [ POST >=> insertToView view
                      PUT >=> updateView view
                      DELETE >=> deleteView view
                    ]*)

        let returnError : UserViewErrorInfo -> WebPart = function
            | UVEArguments msg -> RequestErrors.BAD_REQUEST <| sprintf "Invalid arguments: %s" msg
            | UVEAccessDenied -> RequestErrors.FORBIDDEN ""
            | UVENotFound -> RequestErrors.NOT_FOUND ""
            | UVENotUpdating -> Writers.setHeader "Allow" "GET" >=> RequestErrors.METHOD_NOT_ALLOWED ""
            | UVEParse msg -> RequestErrors.BAD_REQUEST <| sprintf "Parse error: %s" msg
            | UVEResolve msg -> RequestErrors.BAD_REQUEST <| sprintf "Resolution error: %s" msg
            | UVExecute msg -> RequestErrors.BAD_REQUEST <| sprintf "Execution error: %s" msg

        let selectFromView (viewRef : UserViewRef) : WebPart =
            request <| fun req ->
                let rawArgs = req.query |> Seq.mapMaybe (fun (name, maybeArg) -> Option.map (fun arg -> (name, arg)) maybeArg) |> Map.ofSeq
                match rctx.GetUserView(viewRef, rawArgs) with
                    | Ok (cached, res) -> jsonResponse { info = cached.info
                                                         result = res
                                                       }
                    | Result.Error err -> returnError err


        let insertToView (viewRef : UserViewRef) : WebPart =
            RequestErrors.METHOD_NOT_ALLOWED "Not implemented"

        let updateInView (viewRef : UserViewRef) : WebPart =
            RequestErrors.METHOD_NOT_ALLOWED "Not implemented"

        let deleteFromView (viewRef : UserViewRef) : WebPart =
            RequestErrors.METHOD_NOT_ALLOWED "Not implemented"

        let infoView (viewRef : UserViewRef) : WebPart =
            match rctx.GetUserViewInfo(viewRef) with
                | Ok cached -> jsonResponse cached.info
                | Result.Error err -> returnError err

        let viewApi (method : string) (viewRef : UserViewRef) : WebPart =
            match method with
                | "entries" ->
                    choose
                        [ GET >=> selectFromView viewRef
                          POST >=> insertToView viewRef
                          PUT >=> updateInView viewRef
                          DELETE >=> deleteFromView viewRef
                        ]
                | "info" -> GET >=> infoView viewRef
                | _ -> succeed

        let anonymousView method =
            request <| fun req ->
                match req.queryParam "__query" with
                    | Choice1Of2 rawView -> viewApi method <| UVAnonymous rawView
                    | Choice2Of2 _ -> RequestErrors.BAD_REQUEST "Query not specified"
        let namedView (name, method) = viewApi method <| UVNamed name

        return!
            choose
                [ path "/layout" >=> GET >=> jsonResponse rctx.Cache.allowedDatabae
                  pathScan "/views/anonymous/%s" anonymousView
                  pathScan "/views/by_name/%s/%s" namedView
                ] ctx
    }