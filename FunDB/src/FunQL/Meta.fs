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
                Some (AST.SQLName <| sprintf "%O__Foreign__%O" columnName.table.name columnName.name, AST.CMForeignKey tableRef [| (columnName.name, AST.SQLName "Id") |])
            | _ -> None
    (res, constr)

let private makeEntityMeta (tableName : AST.TableRef) (entity : ResolvedEntity) : AST.ObjectMeta seq =
    let idSeq = { AST.schema = tableName.schema; AST.name = AST.SQLName <| sprintf "%O__Seq__Id" tableName.name }
    let idConstraints =
        let name = AST.SQLName <| sprintf "%O__Primary__Id" tableName.name
        let constr = AST.CMPrimaryKey [| AST.SQLName "Id" |]
        seqSingleton (name, constr)
        
    let idColumns =
        let col =
            { AST.colType = AST.VTInt
              AST.nullable = false
              AST.defaultValue = Some <| AST.VEFunc ("nextval", [| AST.VEValue (AST.VObject idSeq) |])
            }
        Seq.singleton (AST.SQLName (funId.ToString()), col)

    let makeColumn (name, field) =
        let columnName = AST.SQLName (name.ToString())
        (columnName, makeColumnFieldMeta { AST.table = tableName; AST.name = columnName } field)
    let makeUniqueConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Unique__%O" tableName.name name, makeUniqueConstraintMeta constr)
    let makeCheckConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Check__%O" tableName.name name, makeCheckConstraintMeta constr)

    let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
    let userColumns = columnObjects |> Seq.map (fun (name, (column, maybeConstr)) -> (name, column))
    let columnConstraints = columnObjects |> seqMapMaybe (fun (name, (column, maybeConstr)) -> maybeConstr)
    let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
    let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

    let res = { AST.columns = Seq.append idColumns userColumns |> Map.ofSeq }
    let constraints = Seq.append (Seq.append (Seq.append idConstraints uniqueConstraints) checkConstraints) columnConstraints
    let objects = [ (tableName, OMTable res); (idSeq, OMSequence) ]
    Seq.append objects (Seq.map (fun (name, constr) -> (name, OMConstraint (tableName, constr))) constraints)

let private makeEntitiesMeta (schemaName : AST.SchemaName option) (entities : Map<EntityName, ResolvedEntity>) : AST.SchemaMeta =
    let makeEntity (name, entity) =
        let tableName = { schema = schemaName; name = SQLName <| name.ToString() }
        makeEntityMeta tableName entity
    let objects = entities |> Map.toSeq |> Seq.map makeEntity |> Seq.concat |> Map.ofSeq
    { objects = objects }

let private makeSchemaMeta (schemaName : AST.SchemaName) (schema : ResolvedSchema) : AST.SchemaMeta =
    makeEntitiesMeta (Some schemaName) schema.entities

let buildFunMeta (layout : Layout) : AST.DatabaseMeta =
    let makeSchema (name, schema) =
        let schemaName = AST.SQLName (name.ToString())
        (Some schemaName, makeSchemaMeta schemaName schema)
        
    let schemas = layout.schemas |> Map.toSeq |> Seq.map makeSchema |> Map.ofSeq
    let systemEntities = makeEntitiesMeta None layout.systemEntities
    { AST.schemas = Map.add None systemEntities schemas }
