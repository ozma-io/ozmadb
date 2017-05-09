namespace FunWithFlags.FunDB

open Npgsql
open FunWithFlags.FunCore

type DBQuery(connectionString : string) =
    let connection = new NpgsqlConnection(connectionString)
    let sqlEscape (str : string) = str.Replace("\"", "\\\"")

    interface System.IDisposable with
        member this.Dispose() =
            connection.Dispose()

    member this.Query(entity : string, columns: seq<string * string>, where: string) : seq<string[]> =
        let columns = Seq.map (fun (name, expr) -> sprintf "%s AS \"%s\"" expr (sqlEscape name)) columns |> String.concat ", " 
        if columns = "" then raise <| new System.ArgumentException("Empty list of columns")
        let whereExpr = if where = "" then "" else sprintf "WHERE %s" where
        use command = new NpgsqlCommand(sprintf "SELECT %s FROM \"%s\" %s" columns entity whereExpr, connection)
        connection.Open()
        use reader = command.ExecuteReader()
        seq { while reader.Read() do
                  yield seq { 0 .. reader.FieldCount - 1 } |> Seq.map (fun i -> reader.[i].ToString()) |> Seq.toArray
            } |> Seq.toList |> List.toSeq
