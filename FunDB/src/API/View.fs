module FunWithFlags.FunDB.API.View

open Suave
open Suave.Filters
open Suave.Operators

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.API.Utils
open FunWithFlags.FunDB.ContextCache
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.FunQL.Info
open FunWithFlags.FunDB.FunQL.Query

[<NoComparison>]
type ViewEntriesGetResponse =
    { info : MergedViewInfo
      result : ExecutedViewExpr
    }

type ViewInfoGetResponse =
    { info : MergedViewInfo
      pureAttributes : ExecutedAttributeMap
      pureColumnAttributes : ExecutedAttributeMap array
    }

let viewsApi (rctx : RequestContext) : WebPart =
    let returnError = function
        | UVEArguments msg -> RequestErrors.BAD_REQUEST <| sprintf "Invalid arguments: %s" msg
        | UVEAccessDenied -> RequestErrors.FORBIDDEN ""
        | UVENotFound -> RequestErrors.NOT_FOUND ""
        | UVEParse msg -> RequestErrors.BAD_REQUEST <| sprintf "Parse error: %s" msg
        | UVEResolve msg -> RequestErrors.BAD_REQUEST <| sprintf "Resolution error: %s" msg
        | UVExecute msg -> RequestErrors.BAD_REQUEST <| sprintf "Execution error: %s" msg

    let selectFromView (viewRef : UserViewRef) =
        request <| fun req ->
            let rawArgs = req.query |> Seq.mapMaybe (fun (name, maybeArg) -> Option.map (fun arg -> (name, arg)) maybeArg) |> Map.ofSeq
            match rctx.GetUserView viewRef rawArgs with
                | Ok (cached, res) -> jsonResponse { info = cached.info
                                                     result = res
                                                   }
                | Result.Error err -> returnError err

    let infoView (viewRef : UserViewRef) =
        match rctx.GetUserViewInfo(viewRef) with
            | Ok cached ->
                let res =
                    { info = cached.info
                      pureAttributes = cached.pureAttributes.attributes
                      pureColumnAttributes = cached.pureAttributes.columnAttributes
                    }
                jsonResponse res
            | Result.Error err -> returnError err

    // FIXME: `method` is fugly; replace framework?
    let viewApi (method : string) (viewRef : UserViewRef) =
        match method with
            | "entries" -> GET >=> selectFromView viewRef
            | "info" -> GET >=> infoView viewRef
            | _ -> succeed

    let anonymousView method =
        request <| fun req ->
            match req.queryParam "__query" with
                | Choice1Of2 rawView -> viewApi method <| UVAnonymous rawView
                | Choice2Of2 _ -> RequestErrors.BAD_REQUEST "Query not specified"
    let namedView (name, method) = viewApi method <| UVNamed name

    choose
        [ pathScan "/views/anonymous/%s" anonymousView
          pathScan "/views/by_name/%s/%s" namedView
        ]