module FunWithFlags.FunDB.API.Auth

open System
open System.Linq
open System.Security.Cryptography
open Microsoft.EntityFrameworkCore
open Newtonsoft.Json

open Suave
open Suave.Filters
open Suave.Operators
open Suave.Writers
open Suave.Response

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.API.Utils

type UserName = string

let private signAlgorithm = Jose.JwsAlgorithm.ES512

type LoginRequest =
    { [<JsonProperty(Required=Required.Always)>]
      username : string
      [<JsonProperty(Required=Required.Always)>]
      password : string
    }

type LoginResponse =
    { token : string
    }

type AuthToken =
    { [<JsonProperty(Required=Required.Always)>]
      sub : UserName
      [<JsonProperty(Required=Required.Always, ItemConverterType=typeof<UnixDateTimeOffsetConverter>)>]
      iat : DateTimeOffset
      [<JsonProperty(Required=Required.Always, ItemConverterType=typeof<UnixDateTimeOffsetConverter>)>]
      exp : DateTimeOffset
    }

let private checkLogin (connectionString : string) (req : LoginRequest) : Async<UserName option> =
    async {
        use conn = new DatabaseConnection(connectionString)
        match! conn.System.Users.Where(fun user -> user.Name = req.username).FirstOrDefaultAsync() with
            | null ->
                eprintfn "Couldn't find user %s" req.username
                return None
            | user ->
                return
                    if (user : User).CheckPassword(req.password)
                    then Some user.Name
                    else
                        eprintfn "Invalid password for user %s" req.username
                        None
    }

let private issueToken (cert : ECDsa) (expirationTime : TimeSpan) (userName : string) : WebPart =
    let time = DateTimeOffset.Now
    let payload =
        { sub = userName
          iat = time
          exp = time + expirationTime
        }
    let token = Jose.JWT.Encode(JsonConvert.SerializeObject payload, cert, signAlgorithm)
    jsonResponse { token = token }

let private auth (connectionString : string) (cert : ECDsa) (expirationTime : TimeSpan) (req : LoginRequest) : WebPart =
    fun ctx -> async {
        match! checkLogin connectionString req with
            | None -> return! RequestErrors.FORBIDDEN "Invalid username or password" ctx
            | Some userName -> return! issueToken cert expirationTime userName ctx
    }

let checkAuth (cert : ECDsa) (expirationTime : TimeSpan) (protectedApi : UserName -> WebPart) : WebPart =
    request <| fun req ->
        let fail =
            setHeader "WWW-Authenticate" "Bearer realm=\"protected\""
                >=> response HTTP_401 (UTF8.bytes HTTP_401.message)
        match req.header "authorization" with
            | Choice1Of2 rawToken ->
                match rawToken.Split(' ') with
                    | [|"Bearer"; token|] ->
                        let maybePayload =
                            try
                                Ok (Jose.JWT.Decode(token, cert, signAlgorithm) |> JsonConvert.DeserializeObject<AuthToken>)
                            with
                                | :? Jose.JoseException as e -> Result.Error <| e.Message
                                | :? JsonException as e -> Result.Error <| e.Message
                                | :? ArgumentOutOfRangeException as e -> Result.Error <| e.Message
                        match maybePayload with
                            | Ok payload ->
                                let time = DateTimeOffset.UtcNow + expirationTime
                                if payload.exp > time then
                                    fail
                                else
                                    protectedApi payload.sub
                            | Result.Error _ -> fail
                    | _ -> fail
            | _ -> fail

let authApi (connectionString : string) (cert : ECDsa) (expirationTime : TimeSpan) (protectedApi : UserName -> WebPart) : WebPart =
    // Check that the certificate is correct
    ignore <| Jose.JWT.Encode("{}", cert, signAlgorithm)
    choose
        [ path "/auth/login" >=> POST >=> jsonRequest (auth connectionString cert expirationTime)
          checkAuth cert expirationTime <| fun userName ->
              choose
                  [ path "/auth/renew" >=> POST >=> issueToken cert expirationTime userName
                    protectedApi userName
                  ]
        ]