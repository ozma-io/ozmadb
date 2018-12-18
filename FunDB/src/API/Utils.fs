module FunWithFlags.FunDB.API.Utils

open Newtonsoft.Json
open Suave
open Suave.Operators
open Suave.Logging

let jsonRequest (jsonApi : 'a -> WebPart) : WebPart =
    request <| fun req ->
        match req.header "content-type" with
            | Choice1Of2 "application/json" | Choice2Of2 _ ->
                let maybePayload =
                    try
                        Ok (req.rawForm |> UTF8.toString |> JsonConvert.DeserializeObject<'a>)
                    with
                        | :? JsonException as e -> Result.Error <| e.Message
                match maybePayload with
                    | Ok p -> jsonApi p
                    | Result.Error msg -> RequestErrors.BAD_REQUEST msg
            | Choice1Of2 msg -> RequestErrors.UNSUPPORTED_MEDIA_TYPE msg

let jsonResponse (payload : 'a) : WebPart =
    let settings =
        new JsonSerializerSettings(
#if DEBUG
            Formatting = Formatting.Indented
#endif
         )
    JsonConvert.SerializeObject(payload, settings) |> Successful.OK >=> Writers.setMimeType "application/json"
