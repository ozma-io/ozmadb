module FunWithFlags.FunDB.API.Info

open Giraffe

open FunWithFlags.FunDB.API.Utils

let infoApi : HttpHandler =
    let ping = GET >=> Successful.OK "Hello"

    choose
        [ route "/check_access" >=> withContext (fun _ -> ping)
          route "/ping" >=> ping
        ]
