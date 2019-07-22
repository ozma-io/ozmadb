module FunWithFlags.FunDB.Operations.Entity

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Flatten
open FunWithFlags.FunDB.Permissions.Entity
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type EntityExecutionException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityExecutionException (message, null)

type EntityDeniedException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = EntityDeniedException (message, null)

type EntityId = int

type EntityArguments = Map<FieldName, FieldValue>

let private runQuery (runFunc : string -> ExprParameters -> Task<'a>) (globalArgs : EntityArguments) (query : Query<'q>) (placeholders : EntityArguments) : Task<'a> =
    task {
        try
            // FIXME: Slow
            let args = Map.unionUnique (Map.mapKeys PGlobal globalArgs) (Map.mapKeys PLocal placeholders)
            return! runFunc (query.expression.ToSQLString()) (prepareArguments query.arguments args)
        with
            | :? QueryException as ex ->
                return raisefWithInner EntityExecutionException ex.InnerException "%s" ex.Message
    }

let private runNonQuery (connection : QueryConnection) = runQuery connection.ExecuteNonQuery

let private runIdQuery (connection : QueryConnection) = runQuery connection.ExecuteIdQuery

let private clearFieldType : ResolvedFieldType -> ArgumentFieldType = function
    | FTReference (r, _) -> FTReference (r, None)
    | FTEnum vals -> FTEnum vals
    | FTType t -> FTType t

let insertEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : FlatRole option) (entityRef : ResolvedEntityRef) (rawArgs : EntityArguments) : Task<int> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None when Option.isSome field.defaultValue -> None
            | None when field.isNullable -> None
            | None -> raisef EntityExecutionException "Required field not provided: %O" fieldName
            | Some arg -> Some (fieldName, { argType = clearFieldType field.fieldType; optional = field.isNullable })

        let entity = layout.FindEntity entityRef |> Option.get
        // FIXME: Lots of shuffling types around; make arguments API better?
        let argumentTypes = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments = argumentTypes |> Seq.map (fun (name, arg) -> (PLocal name, arg)) |> Map.ofSeq |> compileArguments
        // Id is needed so that we always have at least one value inserted.
        let columns = Seq.append (Seq.singleton sqlFunId) (argumentTypes |> Seq.map (fun (name, arg) -> compileName name)) |> Array.ofSeq
        let values = arguments.types |> Map.toSeq |> Seq.map (fun (name, arg) -> SQL.IVValue <| SQL.VEPlaceholder arg.placeholderId)
        let valuesWithId = Seq.append (Seq.singleton SQL.IVDefault) values |> Array.ofSeq

        let expr =
            { name = compileResolvedEntityRef entityRef
              columns = columns
              values = SQL.IValues [| valuesWithId |]
              returning = [| SQL.SCColumn { table = None; name = sqlFunId } |]
            } : SQL.InsertExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleInsert layout role entityRef query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        return! runIdQuery connection globalArgs restricted rawArgs
    }

let private funIdArg = { argType = FTType (FETScalar SFTInt); optional = false }

let updateEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : FlatRole option) (entityRef : ResolvedEntityRef) (id : EntityId) (rawArgs : EntityArguments) : Task<unit> =
    task {
        let getValue (fieldName : FieldName, field : ResolvedColumnField) =
            match Map.tryFind fieldName rawArgs with
            | None -> None
            | Some arg when field.isImmutable -> raisef EntityDeniedException "Field %O is immutable" { entity = entityRef; name = fieldName }
            | Some arg -> Some (fieldName, { argType = clearFieldType field.fieldType; optional = field.isNullable })

        let entity = layout.FindEntity entityRef |> Option.get
        let argumentTypes = entity.columnFields |> Map.toSeq |> Seq.mapMaybe getValue |> Seq.cache
        let arguments' = argumentTypes |> Seq.map (fun (name, arg) -> (PLocal name, arg)) |> Map.ofSeq |> compileArguments
        let arguments = addArgument (PLocal funId) funIdArg arguments'
        let columns = argumentTypes |> Seq.map (fun (name, arg) -> (compileName name, SQL.VEPlaceholder arguments.types.[PLocal name].placeholderId)) |> Map.ofSeq

        let tableRef = compileResolvedEntityRef entityRef
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder arguments.types.[PLocal funId].placeholderId)

        let expr =
            { name = tableRef
              columns = columns
              where = Some whereId
            } : SQL.UpdateExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleUpdate layout role entityRef query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        let! affected = runNonQuery connection globalArgs restricted (Map.add funId (FInt id) rawArgs)
        if affected = 0 then
            raisef EntityDeniedException "Access denied for update"
    }

let deleteEntity (connection : QueryConnection) (globalArgs : EntityArguments) (layout : Layout) (role : FlatRole option) (entityRef : ResolvedEntityRef) (id : EntityId) : Task<unit> =
    task {
        let arguments = addArgument (PLocal funId) funIdArg emptyArguments
        let whereId = SQL.VEEq (SQL.VEColumn { table = None; name = sqlFunId }, SQL.VEPlaceholder arguments.types.[PLocal funId].placeholderId)
        let tableRef = compileResolvedEntityRef entityRef

        let expr =
            { name = tableRef
              where = Some whereId
            } : SQL.DeleteExpr
        let query =
            { expression = expr
              arguments = arguments
            }
        let restricted =
            match role with
            | None -> query
            | Some role ->
                try
                    applyRoleDelete layout role entityRef query
                with
                | :? PermissionsEntityException as e ->
                    raisefWithInner EntityDeniedException e.InnerException "%s" e.Message

        let! affected = runNonQuery connection globalArgs restricted (Map.singleton funId (FInt id))
        if affected = 0 then
            raisef EntityDeniedException "Access denied to delete"
    }