module FunWithFlags.FunDB.FunQL.UsedReferences

// Count used references without accounting for meta columns.

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type UsedReferences =
    { UsedDatabase : UsedDatabase
      UsedArguments : UsedArguments
      HasRestrictedEntities : bool
    }

type ExprInfo =
    { IsLocal : bool
      HasQuery : bool
      HasAggregates : bool
    }

let emptyExprInfo =
    { IsLocal = true
      HasQuery = false
      HasAggregates = false
    }

let private unionExprInfo (a : ExprInfo) (b : ExprInfo) =
    { IsLocal = a.IsLocal && b.IsLocal
      HasQuery = a.HasQuery || b.HasQuery
      HasAggregates = a.HasAggregates || b.HasAggregates
    }

type private UsedReferencesBuilder (layout : ILayoutBits) =
    let mutable usedArguments : UsedArguments = Set.empty
    let mutable usedDatabase : UsedDatabase = emptyUsedDatabase
    let mutable hasRestrictedEntities = false

    let rec addField (extra : ObjectMap) (ref : ResolvedFieldRef) (field : ResolvedFieldInfo) : ExprInfo =
        match field.Field with
        | RComputedField comp ->
            let info =
                computedFieldCases layout extra { ref with Name = field.Name } comp
                |> Seq.map (fun (case, comp) -> buildForFieldExpr comp.Expression)
                |> Seq.fold unionExprInfo emptyExprInfo
            if comp.IsMaterialized then
                emptyExprInfo
            else
                info
        | _ ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
            emptyExprInfo

    and buildForPath (extra : ObjectMap) (ref : ResolvedFieldRef) (asRoot : bool) : (ResolvedEntityRef * PathArrow) list -> ExprInfo = function
        | [] ->
            let entity = layout.FindEntity ref.Entity |> Option.get
            let field = entity.FindField ref.Name |> Option.get
            addField extra ref field
        | (entityRef, arrow) :: paths ->
            usedDatabase <- addUsedFieldRef ref usedFieldSelect usedDatabase
            if not asRoot then
                hasRestrictedEntities <- true
            let info = buildForPath extra { Entity = entityRef; Name = arrow.Name } arrow.AsRoot paths
            { info with IsLocal = false }

    and buildForReference (ref : LinkedBoundFieldRef) : ExprInfo =
        match ref.Ref.Ref with
        | VRColumn _ ->
            match ObjectMap.tryFindType<FieldMeta> ref.Extra with
            | Some { Bound = Some boundInfo } ->
                let pathWithEntities = Seq.zip boundInfo.Path ref.Ref.Path |> Seq.toList
                buildForPath ref.Extra boundInfo.Ref ref.Ref.AsRoot pathWithEntities
            | _ -> emptyExprInfo
        | VRPlaceholder name ->
            usedArguments <- Set.add name usedArguments
            match ObjectMap.tryFindType<ReferencePlaceholderMeta> ref.Extra with
            | Some argInfo when not (Array.isEmpty ref.Ref.Path) ->
                let argRef = { Entity = argInfo.Path.[0]; Name = ref.Ref.Path.[0].Name }
                let pathWithEntities = Seq.zip argInfo.Path ref.Ref.Path |> Seq.skip 1 |> Seq.toList
                buildForPath ref.Extra argRef ref.Ref.AsRoot pathWithEntities
            | _ -> emptyExprInfo

    and buildForResult : ResolvedQueryResult -> unit = function
        | QRAll alias -> ()
        | QRExpr result -> buildForColumnResult result

    and buildForColumnResult (result : ResolvedQueryColumnResult) =
        buildForAttributes result.Attributes
        ignore <| buildForFieldExpr result.Result

    and buildForAttributes (attributes : ResolvedAttributeMap) =
        Map.iter (fun name expr -> ignore <| buildForFieldExpr expr) attributes

    and buildForFieldExpr (expr : ResolvedFieldExpr) : ExprInfo =
        let mutable exprInfo = emptyExprInfo

        let iterReference ref =
            let newExprInfo = buildForReference ref
            exprInfo <- unionExprInfo exprInfo newExprInfo

        let iterQuery query =
            buildForSelectExpr query
            exprInfo <-
                { exprInfo with
                    IsLocal = false
                    HasQuery = true
                }

        let iterAggregate aggr =
            exprInfo <- { exprInfo with HasAggregates = true }

        let mapper =
            { idFieldExprIter with
                FieldReference = iterReference
                Query = iterQuery
                Aggregate = iterAggregate
            }

        iterFieldExpr mapper expr
        exprInfo

    and buildForOrderColumn (ord : ResolvedOrderColumn) =
        buildForFieldExpr ord.Expr |> ignore

    and buildForOrderLimitClause (limits : ResolvedOrderLimitClause) =
        Array.iter buildForOrderColumn limits.OrderBy
        Option.iter (ignore << buildForFieldExpr) limits.Limit
        Option.iter (ignore << buildForFieldExpr) limits.Offset

    and buildForSelectTreeExpr : ResolvedSelectTreeExpr -> unit = function
        | SSelect query -> buildForSingleSelectExpr query
        | SSetOp setOp ->
            buildForSelectExpr setOp.A
            buildForSelectExpr setOp.B
            buildForOrderLimitClause setOp.OrderLimit
        | SValues values ->
            let buildForOne = Array.iter (ignore << buildForFieldExpr)
            Array.iter buildForOne values

    and buildForCommonTableExpr (cte : ResolvedCommonTableExpr) =
        buildForSelectExpr cte.Expr

    and buildForCommonTableExprs (ctes : ResolvedCommonTableExprs) =
        Array.iter (fun (name, expr) -> buildForCommonTableExpr expr) ctes.Exprs

    and buildForSelectExpr (select : ResolvedSelectExpr) =
        Option.iter buildForCommonTableExprs select.CTEs
        buildForSelectTreeExpr select.Tree

    and buildForSingleSelectExpr (query : ResolvedSingleSelectExpr) =
        buildForAttributes query.Attributes
        Option.iter buildForFromExpr query.From
        Option.iter (ignore << buildForFieldExpr) query.Where
        Array.iter (ignore << buildForFieldExpr) query.GroupBy
        Array.iter buildForResult query.Results
        buildForOrderLimitClause query.OrderLimit

    and buildForFromExpr : ResolvedFromExpr -> unit = function
        | FEntity fromEnt ->
            if not fromEnt.AsRoot then
                hasRestrictedEntities <- true
        | FJoin join ->
            buildForFromExpr join.A
            buildForFromExpr join.B
            ignore <| buildForFieldExpr join.Condition
        | FSubExpr subsel -> buildForSelectExpr subsel.Select

    member this.BuildForSelectExpr expr = buildForSelectExpr expr
    member this.BuildForFieldExpr expr = buildForFieldExpr expr

    member this.UsedDatabase = usedDatabase
    member this.UsedArguments = usedArguments
    member this.HasRestrictedEntities = hasRestrictedEntities

let selectExprUsedReferences (layout : ILayoutBits) (expr : ResolvedSelectExpr) : UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    builder.BuildForSelectExpr expr
    { UsedDatabase = builder.UsedDatabase
      UsedArguments = builder.UsedArguments
      HasRestrictedEntities = builder.HasRestrictedEntities
    }

let fieldExprUsedReferences (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ExprInfo * UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    let info = builder.BuildForFieldExpr expr
    let ret =
        { UsedDatabase = builder.UsedDatabase
          UsedArguments = builder.UsedArguments
          HasRestrictedEntities = builder.HasRestrictedEntities
        }
    (info, ret)