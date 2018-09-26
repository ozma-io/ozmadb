module FunWithFlags.FunDB.FunQL.Meta

open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.FunQL.AST

exception FunMetaError of info : string with
    override this.Message = this.info

let private compileDefaultExpr : PureFieldExpr -> AST.PureValueExpr =
    let voidColumn c = raise <| FunMetaError <| sprintf "Unexpected column in default expression: %O" c
    let voidPlaceholder c = raise <| FunMetaError <| sprintf "Unexpected placeholder in default expression: %O" c
    genericCompileFieldExpr voidColumn voidPlaceholder

let private compileCheckExpr : LocalFieldExpr -> AST.LocalValueExpr =
    let compileColumn c = SQLName (c.ToString())
    let voidPlaceholder c = raise <| FunMetaError <| sprintf "Unexpected placeholder in check expression: %O" c
    genericCompileFieldExpr compileColumn voidPlaceholder

let private makeUniqueConstraintMeta (constr : ResolvedUniqueConstraint) : AST.ConstraintMeta =
    CMUnique <| Array.map (fun name -> SQLName (name.ToString())) constr.columns

let private makeCheckConstraintMeta (constr : ResolvedCheckConstraint) : AST.ConstraintMeta =
    CMCheck <| compileCheckExpr constr.expression

let private makeColumnFieldMeta (columnName : AST.ResolvedColumnRef) (field : ResolvedColumnField) : AST.ColumnMeta * (AST.ConstraintName * AST.ConstraintMeta) option =
    let res =
        { AST.columnType = compileFieldType field.fieldType
          AST.isNullable = field.isNullable
          AST.defaultExpr = Option.map compileDefaultExpr field.defaultExpr
        }
    let constr =
        match field.fieldType with
            | FTReference (entityRef, restriction) ->
                // FIXME: support restrictions!
                let tableRef = compileEntityRef entityRef
                Some (AST.SQLName <| sprintf "%O__Foreign__%O" columnName.table.name columnName.name, AST.CMForeignKey [| (columnName.name, tableRef) |])
            | _ -> None
    (res, constr)

let private makeEntityObjects (entityName : EntityName) (entity : ResolvedEntity) : Map<AST.ColumnName, AST.ColumnMeta> * Map<AST.ConstraintName, AST.ConstraintMeta> =
    let tableName = AST.SQLName (entityName.ToString())

    let makeColumn (name, field) =
        let columnName = AST.SQLName (name.ToString())
        (columnName, makeColumnFieldMeta { AST.table = tableName; AST.name = columnName } field)
    let makeUniqueConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Unique__%O" entityName name), makeUniqueConstraintMeta constr)
    let makeCheckConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Check__%O" entityName name), makeCheckConstraintMeta constr)

    let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
    let columns = columnObjects |> Seq.map (fun (name, (column, constr)) -> (name, column)) |> Map.ofSeq
    let columnConstraints = columnObjects |> seqMapMaybe (fun (name, (column, constr)) -> constr)
    let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
    let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

    let constraints = Seq.append columnConstraints (Seq.append uniqueConstraints checkConstraints) |> Map.ofSeq
    (columns, constraints)

let private makeChildEntityObjects (entityName : EntityName) (entity : ResolvedEntity) : Map<AST.ColumnName, AST.ColumnMeta> * Map<AST.ConstraintName, AST.ConstraintMeta> =
    assert Option.isSome <| entity.ancestor
    let (rawColumns, rawConstraints) = makeEntityObjects entityName entity
    let columns = Map.map (fun name column -> { column with isNullable = true }) rawColumns

    let filterConstraint = function
        | AST.CMForeignKey _ -> true
        | AST.CMUnique _ -> true
        | AST.CMCheck _ -> false
    (columns, Map.map (fun (name, constr) -> filterConstraint constr) constraints)
    // FIXME: add CHECK constraints

let private makeUnderlyingEntityMeta (entities : Map<EntityName, ResolvedEntity>) (schema : AST.SchemaName option) (rootEntityName : EntityName) (rootEntity : ResolvedEntity) : AST.SchemaMeta =
    let idSeq = { AST.schema = schema; AST.name = AST.SQLName <| sprintf "%O__Seq__Id" rootEntityName }
    let idConstraints =
        let name = AST.SQLName <| sprintf "%O__Primary__Id" rootEntityName
        let constr = AST.CMPrimaryKey [| AST.SQLName "Id" |]
        seqSingleton (name, constr)
        
    let idColumns =
        let col =
            { AST.colType = AST.VTInt
              AST.nullable = false
              AST.defaultValue = Some (AST.VEFunc ("nextval", [| AST.VEValue (AST.VObject idSeq) |]))
            }
        Seq.singleton (AST.SQLName (funId.ToString()), col)
    // CHECK that discriminator has only allowed values.
    let subEntityColumns =
        if Set.isEmpty rootEntity.descendants
        then Seq.empty
        else
            let col =
                { AST.colType = AST.VTString
                  AST.nullable = false
                  AST.defaultValue = None
                }
            Seq.singleton (AST.SQLName (funSubEntity.ToString()), col)

    let rootObjects = makeEntityObjects rootEntityName rootEntity
    let childObjects = entity.descendantsClosure |> Set.toSeq |> Seq.map (fun name -> makeChildEntityObjects name (Map.find entities name))
    let columnObjects = entitiesClosure 
        (entity.descendants |> entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
    let userColumns = columnObjects |> Seq.map (fun (name, (column, constrs)) -> (name, column))

    let res = { AST.columns = Seq.append (Seq.append idColumns subEntityColumns) userColumns |> Map.ofSeq }
    let constraints = Seq.append (Seq.append (Seq.append idConstraints uniqueConstraints) checkConstraints) foreignConstraints
    (res, constraints)

let private makeEntitiesMeta (schema : AST.SchemaName option) (entities : Map<EntityName, ResolvedEntity>) : AST.SchemaMeta =
    entities
    |> Map.toSeq
    |> Seq.filter (fun (name, entity) -> Option.isNone entity.ancestor)
    |> Seq.map (fun (name, entity) -> makeUnderlyingEntityMeta entities schema name entity)
    |> Seq.fold schemaUnion emptySchema

let private makeSchemaMeta (name : AST.SchemaName) (schema : ResolvedSchema) : AST.SchemaMeta =
    makeEntitiesMeta (Some name) schema.entities

let buildFunMeta (layout : Layout) : AST.DatabaseMeta =
    let makeSchema (name, schema) =
        let schemaName = AST.SQLName (name.ToString())
        (Some schemaName, makeSchemaMeta schemaName schema)
        
    let schemas = layout.schemas |> Map.toSeq |> Seq.map makeSchema |> Map.ofSeq
    let systemEntities = makeEntitiesMeta None layout.systemEntities
    { AST.schemas = Map.add None systemEntities schemas }
