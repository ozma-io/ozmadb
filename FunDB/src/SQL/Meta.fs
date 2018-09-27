module FunWithFlags.FunDB.SQL.Meta

open Microsoft.EntityFrameworkCore
open System.ComponentModel.DataAnnotations.Schema
open System.Collections.Generic

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Lexer
open FunWithFlags.FunDB.SQL.Parser
open FunWithFlags.FunDB.SQL.Array.Lexer
open FunWithFlags.FunDB.SQL.Array.Parser

exception SQLMetaError of info : string with
    override this.Message = this.info

type private InformationSchemaContext (options : DbContextOptions<InformationSchemaContext>) =
    inherit DbContext (options)

    member val Schemata = null : DbSet<Schema> with get, set
    member val Tables = null : DbSet<Table> with get, set
    member val Columns = null : DbSet<Column> with get, set
    member val TableConstraints = null : DbSet<TableConstraint> with get, set
    member val KeyColumnUsage = null : DbSet<KeyColumn> with get, set
    member val ConstraintColumnUsage = null : DbSet<ConstraintColumn> with get, set
    member val Sequences = null : DbSet<Sequence> with get, set

    override this.OnModelCreating (modelBuilder : ModelBuilder) =
        ignore <| modelBuilder.Entity<Schema>()
            .HasKey([| "SchemaCatalog"; "SchemaName" |])
        ignore <| modelBuilder.Entity<Table>()
            .HasKey([| "TableCatalog"; "TableSchema"; "TableName" |])
        ignore <| modelBuilder.Entity<Table>()
            .HasOne(fun table -> table.Schema)
            .WithMany(fun (schema : Schema) -> schema.Tables)
            .HasForeignKey([| "TableCatalog"; "TableSchema" |])
        ignore <| modelBuilder.Entity<Column>()
            .HasOne(fun column -> column.Table)
            .WithMany(fun (table : Table) -> table.Columns)
            .HasForeignKey([| "TableCatalog"; "TableSchema"; "TableName" |])
        ignore <| modelBuilder.Entity<TableConstraint>()
            .HasKey([| "ConstraintCatalog"; "ConstraintSchema"; "ConstraintName" |])
        ignore <| modelBuilder.Entity<TableConstraint>()
            .HasOne(fun constr -> constr.Table)
            .WithMany(fun (table : Table) -> table.TableConstraints)
            .HasForeignKey([| "TableCatalog"; "TableSchema"; "TableName" |])
        ignore <| modelBuilder.Entity<KeyColumn>()
            .HasOne(fun key -> key.TableConstraint)
            .WithMany(fun (constr : TableConstraint) -> constr.KeyColumnUsage)
            .HasForeignKey([| "ConstraintCatalog"; "ConstraintSchema"; "ConstraintName" |])
        ignore <| modelBuilder.Entity<ConstraintColumn>()
            .HasOne(fun col -> col.TableConstraint)
            .WithMany(fun (constr : TableConstraint) -> constr.ConstraintColumnUsage)
            .HasForeignKey([| "ConstraintCatalog"; "ConstraintSchema"; "ConstraintName" |])
        ignore <| modelBuilder.Entity<Sequence>()
            .HasOne(fun seq -> seq.Schema)
            .WithMany(fun (schema : Schema) -> schema.Sequences)
            .HasForeignKey([| "SequenceCatalog"; "SequenceSchema" |])

and
    [<Table("schemata", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private Schema () =
        [<Column("schema_catalog")>]
        member val SchemaCatalog = "" with get, set
        [<Column("schema_name")>]
        member val SchemaName = "" with get, set
        
        member val Tables = null : IEnumerable<Table> with get, set
        member val Sequences = null : IEnumerable<Sequence> with get, set

and
    [<Table("tables", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private Table () =
        [<Column("table_catalog")>]
        member val TableCatalog = "" with get, set
        [<Column("table_schema")>]
        member val TableSchema = "" with get, set
        [<Column("table_name")>]
        member val TableName = "" with get, set
        [<Column("table_type")>]
        member val TableType = "" with get, set

        member val Schema = null : Schema with get, set
        
        member val Columns = null : IEnumerable<Column> with get, set
        member val TableConstraints = null : IEnumerable<TableConstraint> with get, set

and
    [<Table("columns", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private Column () =
        [<Column("table_catalog")>]
        member val TableCatalog = "" with get, set
        [<Column("table_schema")>]
        member val TableSchema = "" with get, set
        [<Column("table_name")>]
        member val TableName = "" with get, set
        [<Column("column_name")>]
        member val ColumnName = "" with get, set
        [<Column("column_default")>]
        member val ColumnDefault = null : string with get, set
        [<Column("is_nullable")>]
        member val IsNullable = "" with get, set
        [<Column("udt_catalog")>]
        member val UdtCatalog = "" with get, set
        [<Column("udt_schema")>]
        member val UdtSchema = "" with get, set
        [<Column("udt_name")>]
        member val UdtName = "" with get, set

        member val Table = null : Table with get, set

and
    [<Table("table_constraints", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private TableConstraint () =
        [<Column("constraint_catalog")>]
        member val ConstraintCatalog = "" with get, set
        [<Column("constraint_schema")>]
        member val ConstraintSchema = "" with get, set
        [<Column("constraint_name")>]
        member val ConstraintName = "" with get, set
        [<Column("table_catalog")>]
        member val TableCatalog = "" with get, set
        [<Column("table_schema")>]
        member val TableName = "" with get, set
        [<Column("table_name")>]
        member val TableName = "" with get, set
        [<Column("constraint_type")>]
        member val ConstraintType = "" with get, set
        
        member val Table = null : Table with get, set
        
        member val KeyColumnUsage = null : IEnumerable<KeyColumn> with get, set
        member val ConstraintColumnUsage = null : IEnumerable<ConstraintColumn> with get, set

and
    [<Table("key_column_usage", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private KeyColumn () =
        [<Column("constraint_catalog")>]
        member val ConstraintCatalog = "" with get, set
        [<Column("constraint_schema")>]
        member val ConstraintSchema = "" with get, set
        [<Column("constraint_name")>]
        member val ConstraintName = "" with get, set
        [<Column("table_catalog")>]
        member val TableCatalog = "" with get, set
        [<Column("table_schema")>]
        member val TableSchema = "" with get, set
        [<Column("table_name")>]
        member val TableName = "" with get, set
        [<Column("column_name")>]
        member val ColumnName = "" with get, set
        [<Column("ordinal_position")>]
        member val OrdinalPosition = 0 with get, set

        member val TableConstraint = null : TableConstraint with get, set

and
    [<Table("constraint_column_usage", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private ConstraintColumn () =
        [<Column("constraint_catalog")>]
        member val ConstraintCatalog = "" with get, set
        [<Column("constraint_schema")>]
        member val ConstraintSchema = "" with get, set
        [<Column("constraint_name")>]
        member val ConstraintName = "" with get, set
        [<Column("table_catalog")>]
        member val TableCatalog = "" with get, set
        [<Column("table_schema")>]
        member val TableSchema = "" with get, set
        [<Column("table_name")>]
        member val TableName = "" with get, set
        [<Column("column_name")>]
        member val ColumnName = "" with get, set

        member val TableConstraint = null : TableConstraint with get, set

and
    [<Table("sequences", Schema="information_schema")>]
    [<AllowNullLiteral>]
    private Sequence () =
        [<Column("sequence_catalog")>]
        member val SequenceCatalog = "" with get, set
        [<Column("sequence_schema")>]
        member val SequenceSchema = "" with get, set
        [<Column("sequence_name")>]
        member val SequenceName = "" with get, set
        
        member val Schema = null : Schema with get, set
    
let private parseConstraintType (str : string) : ConstraintType option =
    match str.ToUpper() with
        | "UNIQUE" -> Some CTUnique
        | "CHECK" -> Some CTCheck
        | "PRIMARY KEY" -> Some CTPrimaryKey
        | "FOREIGN KEY" -> Some CTForeignKey
        | _ -> None

let private tryRegclass (str : string) : SchemaObject option =
    match parse tokenizeSQL schemaObject str with
        | Ok obj -> Some obj
        | Error msg -> None

let private makeTableFromName (schema : string) (table : string) : TableRef =
    { schema = if schema = publicSchema.ToString() then None else Some (SQLName schema)
      name = SQLName table
    }

let private makeColumnFromName (schema : string) (table : string) (column : string) : ResolvedColumnRef =
    { table = makeTableFromName schema table
      name = SQLName column
    }

// Convert string-cast patterns into actual types so that we reverse lost type information.
// Don't reduce the expressions beyond that!
let private normalizeDefaultExpr : ParsedValueExpr -> PureValueExpr =
    let rec traverse = function
        | VECast (VEValue (VString str), typ) ->
            let castExpr () = VECast (VEValue (VString str), typ)
            match typ with
                | VTArray scalarType ->
                    match parse tokenizeArray stringArray str with
                        | Error msg -> raise (SQLMetaError <| sprintf "Cannot parse array: %s" msg)
                        | Ok array ->
                            let runArrayCast (castFunc : string -> 'a option) : ValueArray<'a> =
                                let runCast (str : string) =
                                    match castFunc str with
                                        | Some v -> v
                                        | None -> raise (SQLMetaError <| sprintf "Cannot cast array value to type %O: %s" scalarType str)
                                mapValueArray runCast array
                                
                            match findSimpleType scalarType with
                                | None -> castExpr ()
                                | Some STString -> VEValue <| VStringArray array
                                | Some STInt -> VEValue (VIntArray <| runArrayCast tryIntInvariant)
                                | Some STBool -> VEValue (VBoolArray <| runArrayCast tryBool)
                                | Some STDateTime -> VEValue (VDateTimeArray <| runArrayCast tryDateTimeInvariant)
                                | Some STDate -> VEValue (VDateArray <| runArrayCast tryDateTimeInvariant)
                                | Some STRegclass -> VEValue (VRegclassArray <| runArrayCast tryRegclass)
                | VTScalar scalarType ->
                    let runCast castFunc =
                        match castFunc str with
                            | Some v -> v
                            | None -> raise (SQLMetaError <| sprintf "Cannot cast scalar value to type %O: %s" scalarType str)

                    match findSimpleType scalarType with
                        | None -> castExpr ()
                        | Some STString -> castExpr ()
                        | Some STInt -> VEValue (VInt <| runCast tryIntInvariant)
                        | Some STBool -> VEValue (VBool <| runCast tryBool)
                        | Some STDateTime -> VEValue (VDateTime <| runCast tryDateTimeInvariant)
                        | Some STDate -> VEValue (VDate <| runCast tryDateTimeInvariant)
                        | Some STRegclass -> VEValue (VRegclass <| runCast tryRegclass)
        | VEValue value -> VEValue value
        | VEColumn c -> raise (SQLMetaError <| sprintf "Invalid reference in default expression: %O" c)
        | VEPlaceholder i -> raise (SQLMetaError <| sprintf "Invalid placeholder in default expression: %i" i)
        | VENot e -> VENot (traverse e)
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
        | VELike (e, pat) -> VELike (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VEIsNull e -> VEIsNull (traverse e)
        | VEIsNotNull e -> VEIsNotNull (traverse e)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEFunc (name,  args) -> VEFunc (name, Array.map traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
    traverse

let parseUdtName (str : string) =
    if str.StartsWith("_") then
        VTArray <| SQLName (str.Substring(1))
    else
        VTScalar <| SQLName str

let private makeColumnMeta (column : Column) : ColumnName * ColumnMeta =
    let columnType =
        if column.UdtSchema <> "pg_catalog" then
            raise (SQLMetaError <| sprintf "Unsupported user-defined data type: %s.%s" column.UdtSchema column.UdtName)
        parseUdtName column.UdtName
    let defaultExpr =
        if column.ColumnDefault = null
        then None
        else
            match parse tokenizeSQL valueExpr column.ColumnDefault with
                | Ok expr -> Some <| normalizeDefaultExpr expr
                | Error msg -> raise (SQLMetaError <| sprintf "Cannot parse column default value: %s" msg)
    let isNullable =
        match tryBool column.IsNullable with
            | Some v -> v
            | None -> raise (SQLMetaError <| sprintf "Invalid is_nullable value: %s" column.IsNullable)
    let res =
        { columnType = columnType
          isNullable = isNullable
          defaultExpr = defaultExpr
        }
    (SQLName column.ColumnName, res)

let private makeConstraintMeta (constr : TableConstraint) : ConstraintName * ConstraintMeta =
    let keyColumns () =
        constr.KeyColumnUsage |> Seq.sortBy (fun key -> key.OrdinalPosition) |> Seq.map (fun key -> SQLName key.ColumnName) |> Seq.toArray
    let constraintColumns () =
        // We don't have any key to sort on so we pray that PostgreSQL's result order is good. Re: move to pg_catalog.
        constr.ConstraintColumnUsage |> Seq.map (fun col -> makeColumnFromName col.TableSchema col.TableName col.ColumnName) |> Seq.toArray
    let res =
        match parseConstraintType constr.ConstraintType with
            | None -> raise (SQLMetaError <| sprintf "Unknown constraint type: %s" constr.ConstraintType)
            | Some CTUnique -> CMUnique (keyColumns ())
            | Some CTPrimaryKey -> CMPrimaryKey (keyColumns ())
            | Some CTCheck -> failwith "Not implemented yet"
            | Some CTForeignKey ->
                let cols = constraintColumns ()
                ignore <| seqFold1 (fun a b -> if a.table <> b.table then raise (SQLMetaError <| sprintf "Different column tables in a foreign key: %O vs %O" a.table b.table) else b) cols
                let ref = cols.[0].table
                CMForeignKey (ref, Array.zip (keyColumns ()) (Array.map (fun a -> a.name) cols))
    (SQLName constr.ConstraintName, res)

let private makeTableMeta (table : Table) : TableName * TableMeta * (ConstraintName * ConstraintMeta) seq =
    if table.TableType.ToUpper() <> "BASE TABLE" then
        raise (SQLMetaError <| sprintf "Unsupported table type: %s" table.TableType)
    let constraints = table.TableConstraints |> Seq.map makeConstraintMeta
    let res = { columns = table.Columns |> Seq.map makeColumnMeta |> mapOfSeqUnique }
    (SQLName table.TableName, res, constraints)

let private makeSchemaMeta (schema : Schema) : SchemaName option * SchemaMeta =
    let tableObjects = schema.Tables |> Seq.map makeTableMeta |> Seq.cache
    let tables = tableObjects |> Seq.map (fun (name, table, constraints) -> (name, OMTable table)) |> mapOfSeqUnique
    let constraints =
        tableObjects
        |> Seq.map (fun (name, table, constraints) -> Seq.map (fun (constrName, constr) -> (constrName, OMConstraint constr)) constraints)
        |> Seq.concat
        |> mapOfSeqUnique
    let sequences = schema.Sequences |> Seq.map makeSequenceMeta |> Seq.map (fun name -> (name, Sequence)) |> mapOfSeqUnique
    let name = if schema.SchemaName = "public" then None else Some (SQLName schema.SchemaName)
    let res = { objects = mapUnionUnique (mapUnionUnique tables constraints) sequences }
    (name, res)

let buildDatabaseMeta (loggerFactory : ILoggerFactory) (connectionString : string) : DatabaseMeta =
    let dbOptions =
        (DbContextOptionsBuilder<InformationSchemaContext> ())
            .UseNpgsql(connectionString)
            .UseLoggerFactory(loggerFactory)
    use db = new InformationSchemaContext(dbOptions.Options)

    let systemSchemas = set [| "information_schema" "pg_catalog" |]
    let schemas =
        db.Schemata.Where(fun schema -> not Set.contains schema.SchemaName systemSchemas)
            .Include(fun schema -> schema.Tables)
                .ThenInclude(fun table -> table.Columns)
            .Include(fun schema -> schema.Tables)
                .ThenInclude(fun table -> table.TableConstraints)
                    .ThenInclude(fun constr -> constr.KeyColumnUsage)
            .Include(fun schema -> schema.Tables)
                .ThenInclude(fun table -> table.TableConstraints)
                    .ThenInclude(fun constr -> constr.ConstraintColumnUsage)
            .Include(fun schema -> schema.Sequences)

    // We ignore catalog, assuming a database is constant -- it should be so according to PostgreSQL.
    { schemas = schemas |> Seq.map getSchemaMeta |> mapOfSeqUnique }
