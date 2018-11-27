open System
open System.IO
open System.Security.Cryptography.X509Certificates
open System.Linq
open Newtonsoft.Json
open Suave
open Suave.CORS
open Suave.Operators
open Suave.Filters

open FunWithFlags.FunDB.Json
open FunWithFlags.FunDB.Schema
open FunWithFlags.FunDB.Layout.Json
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.Connection
open FunWithFlags.FunDB.Context
open FunWithFlags.FunDB.API.Auth
open FunWithFlags.FunDB.API.View
open FunWithFlags.FunDB.API.Permissions
open FunWithFlags.FunDB.API.Entity
open FunWithFlags.FunDB.ContextCache

type Config =
    { [<JsonProperty(Required=Required.Always)>]
      connectionString : string
      [<JsonProperty(Required=Required.Always)>]
      serverCert : string
      [<JsonProperty(Required=Required.Always)>]
      expirationTime : int
      [<JsonProperty(Required=Required.Always)>]
      host : string
      [<JsonProperty(Required=Required.Always)>]
      port : int
      [<JsonProperty(Required=Required.Always)>]
      preloadLayout : string option
    }

let randomPassword (passwordLength : int) : string =
    let allowedChars = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNOPQRSTUVWXYZ0123456789!@$?_-"
    let rd = new Random()
    Seq.init passwordLength (fun _ -> allowedChars.[rd.Next(0, String.length allowedChars)]) |> Seq.toArray |> System.String

[<EntryPoint>]
let main (args : string array) : int =
    // Register a global converter to have nicer native F# types JSON conversion
    JsonConvert.DefaultSettings <- fun () ->
        new JsonSerializerSettings(
            Converters = [| new UnionConverter() |]
        )

    let configPath = args.[0]
    let rawConfig = File.ReadAllText(configPath)
    let config = JsonConvert.DeserializeObject<Config>(rawConfig)

    use cert = new X509Certificate2(config.serverCert)
    let expirationTime = TimeSpan(0, 0, config.expirationTime)
    let preloadLayout = Option.map (fun path -> parseJsonLayout <| File.ReadAllText(path)) config.preloadLayout

    let cacheStore = ContextCacheStore(config.connectionString, preloadLayout)
        
    using (new DatabaseConnection(config.connectionString)) <| fun conn ->
        // Create admin user
        match conn.System.Users.Where(fun x -> x.Name = rootUserName) |> Seq.first with
            | None ->
                let password = randomPassword 16
                let newUser = new User(Name=rootUserName, Password=password)
                ignore <| conn.System.Users.Add(newUser)
                ignore <| conn.System.SaveChanges()
                eprintfn "Created root user with password '%s'. Please change the password as soon as possible!" password
            | Some user -> ()
 
        conn.Commit()

    let protectedApi (userName : string) =
        fun ctx -> async {
            use rctx = new RequestContext(cacheStore, userName)
            return! choose
                [ viewsApi rctx
                  permissionsApi rctx
                  entitiesApi rctx
                ] ctx
        }

    let suaveCfg =
        { defaultConfig with
            bindings = [ HttpBinding.createSimple HTTP config.host config.port ]
        }
    let corsCfg =
        { defaultCORSConfig with
            allowedMethods = InclusiveOption.Some [ HttpMethod.GET; HttpMethod.PUT; HttpMethod.POST; HttpMethod.OPTIONS ]
        }
    let corsApi = cors corsCfg

    let funApi = choose [
        // FIXME: invalid use of OPTIONS
        OPTIONS >=> corsApi
        corsApi >=> authApi config.connectionString (cert.GetECDsaPrivateKey()) expirationTime protectedApi
        RequestErrors.NOT_FOUND ""
    ]
    startWebServer suaveCfg funApi

    0
