namespace FunWithFlags.FunDB

open System.Runtime.InteropServices
open Npgsql
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunCore

type SortOrder = Asc | Desc

type DBQuery(connectionString : string, userConnectionString : string) =
    let connection = new NpgsqlConnection(userConnectionString)
    let systemConnection = new NpgsqlConnection(connectionString)
    let db = new DatabaseContext((new DbContextOptionsBuilder<DatabaseContext>()).UseNpgsql(connectionString).Options)

    let sqlEscape (str : string) = str.Replace("\"", "\\\"")

    interface System.IDisposable with
        member this.Dispose() =
            connection.Dispose()
            systemConnection.Dispose()
            db.Dispose()
           
    member this.Database = db

    member this.Query
        (
            entity : string,
            columns : seq<string>,
            [<Optional; DefaultParameterValue(null)>] ?where : string,
            [<Optional; DefaultParameterValue(null)>] ?orderBy : seq<string>,
            [<Optional; DefaultParameterValue(null)>] ?orderDirection : SortOrder
        ) : seq<string[]> =

        let columns = String.concat ", " columns
        if columns = "" then raise <| new System.ArgumentException("Empty list of columns")
        let whereVal = defaultArg where ""
        let whereExpr = if whereVal = "" then "" else sprintf "WHERE %s" whereVal
        let orderList = defaultArg orderBy (List.toSeq [])
        let orderExpr =
            if Seq.length orderList = 0
            then ""
            else
                let orderDir =
                    match defaultArg orderDirection Asc with
                    | Asc -> "ASC"
                    | Desc -> "DESC"
                sprintf "ORDER BY %s %s" (String.concat ", " orderList) orderDir

        let isSystem = (entity.[0] = '#')
        let entityName = if isSystem then entity.Remove(0, 1) else entity
        let con = if isSystem then systemConnection else connection
        use command = new NpgsqlCommand(sprintf "SELECT %s FROM \"%s\" %s %s" columns entityName whereExpr orderExpr, con)

        con.Open()
        use reader = command.ExecuteReader()
        seq { while reader.Read() do
                  yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i].ToString()) |> Seq.toArray
            } |> Seq.toList |> List.toSeq
