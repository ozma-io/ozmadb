module FunWithFlags.FunDB.FunQL.UsedReferences

// Count used references without accounting for meta columns.

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.Utils
module SQL = FunWithFlags.FunDB.SQL.AST

type UsedReferences =
    { UsedSchemas : UsedSchemas
      UsedArguments : UsedArguments
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
    let mutable usedSchemas : UsedSchemas = Map.empty

    let rec addField (extra : ObjectMap) (ref : ResolvedFieldRef) (field : ResolvedFieldInfo) : ExprInfo =
        match field.Field with
        | RComputedField comp ->
            match comp.VirtualCases with
            | None -> buildForFieldExpr comp.Expression
            | Some cases ->
                computedFieldCases layout extra ref.Name cases
                |> Seq.map (fun (case, expr) -> buildForFieldExpr expr)
                |> Seq.fold unionExprInfo emptyExprInfo
        | _ ->
            usedSchemas <- addUsedFieldRef ref usedSchemas
            emptyExprInfo

    and buildForPath (extra : ObjectMap) (ref : ResolvedFieldRef) : (ResolvedEntityRef * FieldName) list -> ExprInfo = function
        | [] ->
            let entity = layout.FindEntity ref.Entity |> Option.get
            let field = entity.FindField ref.Name |> Option.get
            addField extra ref field
        | (entityRef, name) :: paths ->
            usedSchemas <- addUsedFieldRef ref usedSchemas
            let info = buildForPath extra { Entity = entityRef; Name = name } paths
            { info with IsLocal = false }

    and buildForReference (ref : LinkedBoundFieldRef) : ExprInfo =
        match ref.Ref.Ref with
        | VRColumn _ ->
            match ObjectMap.tryFindType<FieldMeta> ref.Extra with
            | Some { Bound = Some boundInfo } ->
                let pathWithEntities = Seq.zip boundInfo.Path ref.Ref.Path |> Seq.toList
                buildForPath ref.Extra boundInfo.Ref pathWithEntities
            | _ -> emptyExprInfo
        | VRPlaceholder name ->
            usedArguments <- Set.add name usedArguments
            match ObjectMap.tryFindType<ReferencePlaceholderMeta> ref.Extra with
            | Some argInfo when not (Array.isEmpty ref.Ref.Path) ->
                let argRef = { Entity = argInfo.Path.[0]; Name = ref.Ref.Path.[0] }
                let pathWithEntities = Seq.zip argInfo.Path ref.Ref.Path |> Seq.skip 1 |> Seq.toList
                buildForPath ref.Extra argRef pathWithEntities
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
        | FEntity (pun, name) -> ()
        | FJoin join ->
            buildForFromExpr join.A
            buildForFromExpr join.B
            ignore <| buildForFieldExpr join.Condition
        | FSubExpr (name, q) -> buildForSelectExpr q

    member this.BuildForSelectExpr expr = buildForSelectExpr expr
    member this.BuildForFieldExpr expr = buildForFieldExpr expr

    member this.UsedSchemas = usedSchemas
    member this.UsedArguments = usedArguments

let selectExprUsedReferences (layout : ILayoutBits) (expr : ResolvedSelectExpr) : UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    builder.BuildForSelectExpr expr
    { UsedSchemas = builder.UsedSchemas
      UsedArguments = builder.UsedArguments
    }

let fieldExprUsedReferences (layout : ILayoutBits) (expr : ResolvedFieldExpr) : ExprInfo * UsedReferences =
    let builder = UsedReferencesBuilder (layout)
    let info = builder.BuildForFieldExpr expr
    let ret =
        { UsedSchemas = builder.UsedSchemas
          UsedArguments = builder.UsedArguments
        }
    (info, ret)