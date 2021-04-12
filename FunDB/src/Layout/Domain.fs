module FunWithFlags.FunDB.Layout.Domain

open System.Security.Cryptography

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DML

// Field domains, that is, all possible values for a field considering check constraints.

// Right now all domains are SQL expressions with `value` and `pun` columns. This may change in future.
type DomainExpr =
    { Query : Query<SQL.SelectExpr>
      UsedSchemas : UsedSchemas
      Hash : string
    }

type FieldDomain =
    { Generic : DomainExpr
      RowSpecific : DomainExpr option
    }

type EntityDomains =
    { Fields : Map<FieldName, FieldDomain>
    }

type SchemaDomains =
    { Entities : Map<EntityName, EntityDomains>
    }

type LayoutDomains =
    { Schemas : Map<SchemaName, SchemaDomains>
    } with
        member this.FindEntity (ref : ResolvedEntityRef) =
            match Map.tryFind ref.schema this.Schemas with
            | None -> None
            | Some schema -> Map.tryFind ref.name schema.Entities

        member this.FindField (ref : ResolvedFieldRef) =
            match this.FindEntity ref.entity with
            | None -> None
            | Some entity -> Map.tryFind ref.name entity.Fields

// If domain depends on values in local row (that is, will be different for different rows).
let private hasLocalDependencies (ref : ResolvedFieldRef) (usedSchemas : UsedSchemas) : bool =
    let localEntity = usedSchemas |> Map.find ref.entity.schema |> Map.find ref.entity.name
    // We always have at least one field in used local fields: the reference itself. Hence we just check that size is larger than 1.
    Set.count localEntity > 1

let private makeResultExpr (ref : FieldRef) : ResolvedFieldExpr =
    let resultColumn =
        { Ref =
            VRColumn
                { Ref = ref
                  Bound = None
                }
          Path = [||]
        }
    FERef resultColumn

let private makeResultColumn (name : FieldName option) (expr : ResolvedFieldExpr) : ResolvedQueryResult =
    let col =
        { Attributes = Map.empty
          Result = expr
          Alias = name
        }
    QRExpr col

let domainValueName = FunQLName "value"
let sqlDomainValueName = compileName domainValueName

let domainPunName = FunQLName "pun"
let sqlDomainPunName = compileName domainPunName

let rowName = FunQLName "row"
let rowEntityRef : EntityRef = { schema = None; name = rowName }
let referencedName = FunQLName "referenced"
let referencedEntityRef : EntityRef = { schema = None; name = referencedName }

// Generic check is the one that only has has references to one field, itself of reference type.
// For example, `user_id=>account_id=>balance > 0`.
// Convert this to just `account_id=>balance > 0`, so that we can use it SELECT for referenced entity.
//
// Row-specific check is the one that has both references to the field we build domain for, as well as other local fields.
// We split them into references to two separate entities: `row` and `referenced`.
let private renameDomainCheck (refEntityRef : ResolvedEntityRef) (refFieldName : FieldName) (expr : ResolvedFieldExpr) : ResolvedFieldExpr =
    let convertRef : LinkedBoundFieldRef -> LinkedBoundFieldRef = function
        | { Ref = VRColumn { Ref = { entity = None; name = fieldName }; Bound = Some bound }; Path = path } ->
            if fieldName = refFieldName then
                // Referenced entity.
                if Array.isEmpty path then
                    let bound =
                        { Ref = { entity = refEntityRef; name = funId }
                          Immediate = true
                        }
                    { Ref = VRColumn { Ref = { entity = Some referencedEntityRef; name = funId }; Bound = Some bound }; Path = [||] }
                else
                    let name = path.[0]
                    let bound =
                        { Ref = { entity = refEntityRef; name = name }
                          Immediate = true
                        }
                    { Ref = VRColumn { Ref = { entity = Some referencedEntityRef; name = name }; Bound = Some bound }; Path = Array.skip 1 path }
            else
                { Ref = VRColumn { Ref = { entity = Some rowEntityRef; name = fieldName }; Bound = Some bound }; Path = path }
        | ref -> failwithf "Impossible reference: %O" ref
    let convertQuery query = failwithf "Impossible query: %O" query
    mapFieldExpr (idFieldExprMapper convertRef convertQuery) expr

let private compileReferenceOptionsSelectFrom (layout : Layout) (arguments : QueryArguments) (from : ResolvedFromExpr) (where : ResolvedFieldExpr option) : DomainExpr =
    let idExpr = makeResultExpr { entity = Some referencedEntityRef; name = funId }
    let mainExpr = makeResultExpr { entity = Some referencedEntityRef; name = funMain }
    let orderByMain =
        { emptyOrderLimitClause with
              OrderBy = [| (Asc, mainExpr) |]
        }
    let single =
        { Attributes = Map.empty
          Results = [| makeResultColumn (Some domainValueName) idExpr; makeResultColumn (Some domainPunName) mainExpr |]
          From = Some from
          Where = where
          GroupBy = [||]
          OrderLimit = orderByMain
          Extra = null
        }
    let select =
        { CTEs = None
          Tree = SSelect single
          Extra = null
        }
    let (usedSchemas, expr) = compileSelectExpr layout arguments.Types select
    let hash = expr |> string |> Hash.sha1OfString |> String.hexBytes
    let query =
        { Expression = expr
          Arguments = arguments
        }
    { UsedSchemas = usedSchemas
      Query = query
      Hash = hash
    }

let private compileGenericReferenceOptionsSelect (layout : Layout) (refEntityRef : ResolvedEntityRef) (where : ResolvedFieldExpr option) : DomainExpr =
    let from = FEntity (Some referencedName, relaxEntityRef refEntityRef)
    compileReferenceOptionsSelectFrom layout emptyArguments from where

let private compileRowSpecificReferenceOptionsSelect (layout : Layout) (entityRef : ResolvedEntityRef) (refEntityRef : ResolvedEntityRef) (extraWhere : ResolvedFieldExpr option) : DomainExpr =
    let rowFrom = FEntity (None, relaxEntityRef entityRef)
    let refFrom = FEntity (Some referencedName, relaxEntityRef refEntityRef)
    // This could be rewritten to use CROSS JOIN.
    let join =
        { Type = Outer
          A = rowFrom
          B = refFrom
          Condition = FEValue (FBool true)
        }
    let argumentInfo =
        { ArgType = FTReference entityRef
          Optional = false
        } : ResolvedArgument
    let placeholder = PLocal funId
    let (argId, arguments) = addArgument placeholder argumentInfo emptyArguments
    let idCol : ResolvedFieldExpr = FERef { Ref = VRColumn { Ref = { entity = Some { schema = None; name = rowName }; name = funId }; Bound = None }; Path = [||] }
    let argRef : ResolvedFieldExpr = FERef { Ref = VRPlaceholder placeholder; Path = [||] }
    let idCheck = FEBinaryOp (idCol, BOEq, argRef)
    let where =
        match extraWhere with
        | None -> idCheck
        | Some where -> FEAnd (idCheck, where)

    compileReferenceOptionsSelectFrom layout arguments (FJoin join) (Some where)

// For now, we only build domains based on immediate check constraints.
// For example, for check constraint `user_id=>account_id=>balance > 0` we restrict `user_id` based on that.
// However, we could also restrict `account_id`. This is to be implemented yet.
// We, however, also build row-specific checks for constraints such as `user_id=>account_id=>balance > my_balance`.
type private DomainsBuilder (layout : Layout) =
    let mutable fullSelectsCache : Map<ResolvedEntityRef, DomainExpr> = Map.empty

    let buildFullSelect (refEntityRef : ResolvedEntityRef) =
        match Map.tryFind refEntityRef fullSelectsCache with
        | Some cached -> cached
        | None ->
            let ret = compileGenericReferenceOptionsSelect layout refEntityRef None
            fullSelectsCache <- Map.add refEntityRef ret fullSelectsCache
            ret

    let buildReferenceFieldDomain (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (refEntityRef : ResolvedEntityRef) : FieldDomain =
        let (rowSpecificChecks, genericChecks) =
            entity.CheckConstraints
            |> Map.values
            |> Seq.filter (fun constr -> isFieldUsed fieldRef constr.UsedSchemas)
            |> Seq.partition (fun constr -> hasLocalDependencies fieldRef constr.UsedSchemas)
        let genericCheck =
            if Seq.isEmpty genericChecks then
                None
            else
                genericChecks |> Seq.map (fun constr -> renameDomainCheck refEntityRef fieldRef.name constr.Expression) |> Seq.fold1 (curry FEAnd) |> Some
        let genericExpr =
            match genericCheck with
            | None -> buildFullSelect refEntityRef
            | Some check -> compileGenericReferenceOptionsSelect layout refEntityRef (Some check)
        let rowSpecificExpr =
            if Seq.isEmpty rowSpecificChecks then
                None
            else
                let rowSpecificCheck = rowSpecificChecks |> Seq.map (fun constr -> renameDomainCheck refEntityRef fieldRef.name constr.Expression) |> Seq.fold1 (curry FEAnd)
                let fullCheck =
                    match genericCheck with
                    | None -> rowSpecificCheck
                    | Some check -> FEAnd (check, rowSpecificCheck)
                Some <| compileRowSpecificReferenceOptionsSelect layout fieldRef.entity refEntityRef (Some fullCheck)

        { Generic = genericExpr
          RowSpecific = rowSpecificExpr
        }

    let buildFieldDomain (entity : ResolvedEntity) (fieldRef : ResolvedFieldRef) (field : ResolvedColumnField) : FieldDomain option =
        match field.FieldType with
        | FTReference refEntityRef -> Some <| buildReferenceFieldDomain entity fieldRef refEntityRef
        // We can also potentially make domains for enums and booleans.
        | _ -> None

    let buildEntityDomains (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : EntityDomains option =
        let mapField name field =
            let ref = { entity = entityRef; name = name }
            buildFieldDomain entity ref field
        let res = entity.ColumnFields |> Map.mapMaybe mapField
        if Map.isEmpty res then
            None
        else
            Some { Fields = res }

    let buildSchemaDomains (schemaName : SchemaName) (schema : ResolvedSchema) : SchemaDomains option =
        let mapEntity name entity =
            if entity.IsHidden then
                None
            else
                let ref = { schema = schemaName; name = name }
                buildEntityDomains ref entity
        let res = schema.Entities |> Map.mapMaybe mapEntity
        if Map.isEmpty res then
            None
        else
            Some { Entities = res }

    let buildLayoutDomains () : LayoutDomains =
        let res = layout.Schemas |> Map.mapMaybe buildSchemaDomains
        { Schemas = res
        }

    member this.BuildLayoutDomains () = buildLayoutDomains ()

let buildLayoutDomains (layout : Layout) : LayoutDomains =
    let builder = DomainsBuilder layout
    builder.BuildLayoutDomains ()