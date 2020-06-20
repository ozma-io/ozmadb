module FunWithFlags.FunDB.API.Info

open Giraffe

open FunWithFlags.FunDB.API.Utils

let infoApi : HttpHandler =
    let ping = GET >=> (Map.empty |> json |> Successful.ok)

    choose
        [ route "/check_access" >=> withContext (fun _ -> ping)
          route "/ping" >=> ping
        ]
