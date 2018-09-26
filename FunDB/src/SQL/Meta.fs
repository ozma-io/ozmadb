module FunWithFlags.FunDB.SQL.Meta

open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Lexer
open FunWithFlags.FunDB.SQL.Parser
open FunWithFlags.FunDB.SQL.Array.Lexer
open FunWithFlags.FunDB.SQL.Array.Parser
open FunWithFlags.FunDB.SQL.Qualifier

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
        modelBuilder.Entity<Table>()
            .HasOne(fun table -> table.Schema)
            .WithMany(fun schema -> schema.Tables)
            .HasForeignKey(fun table -> (table.TableCatalog, table.TableSchema))
            .HasPrincipalKey(fun schema -> (schema.SchemaCatalog, schema.SchemaName))
        modelBuilder.Entity<Column>()
            .HasOne(fun column -> column.Table)
            .WithMany(fun table -> table.Columns)
            .HasForeignKey(fun column -> (column.TableCatalog, column.TableSchema, column.TableName))
            .HasPrincipalKey(fun table -> (table.TableCatalog, table.TableSchema, table.TableName))
        modelBuilder.Entity<TableConstraint>()
            .HasOne(fun constr -> constr.Table)
            .WithMany(fun table -> table.Columns)
            .HasForeignKey(fun constr -> (column.TableCatalog, column.TableSchema, column.TableName))
            .HasPrincipalKey(fun table -> (table.TableCatalog, table.TableSchema, table.TableName))
        modelBuilder.Entity<KeyColumn>()
            .HasOne(fun key -> key.TableConstraint)
            .WithMany(fun constr -> constr.KeyColumnUsage)
            .HasForeignKey(fun key -> (key.ConstraintCatalog, key.ConstraintSchema, key.ConstraintName))
            .HasPrincipalKey(fun constr -> (constr.ConstraintCatalog, constr.ConstraintSchema, constr.ConstraintName))
        modelBuilder.Entity<ConstraintColumn>()
            .HasOne(fun col -> col.TableConstraint)
            .WithMany(fun constr -> constr.ConstraintColumnUsage)
            .HasForeignKey(fun col -> (col.ConstraintCatalog, col.ConstraintSchema, col.ConstraintName))
            .HasPrincipalKey(fun constr -> (constr.ConstraintCatalog, constr.ConstraintSchema, constr.ConstraintName))
        modelBuilder.Entity<Sequence>()
            .HasOne(fun seq -> seq.Schema)
            .WithMany(fun schema -> schema.Sequences)
            .HasForeignKey(fun seq -> (seq.SequenceCatalog, seq.SequenceSchema))
            .HasPrincipalKey(fun schema -> (schema.SchemaCatalog, schema.SchemaName))

[<Table("schemata", Schema="information_schema")>]
and private Schema () =
    [<Column("schema_catalog")>]
    member val SchemaCatalog = "" with get, set
    [<Column("schema_name")>]
    member val SchemaName = "" with get, set

    member val Tables = null : List<Table> with get, set
    member val Sequences = null : List<Sequence> with get, set

[<Table("tables", Schema="information_schema")>]
and private Table () =
    [<Column("table_catalog")>]
    member val TableCatalog = "" with get, set
    [<Column("table_schema")>]
    member val TableSchema = "" with get, set
    [<Column("table_name")>]
    member val TableName = "" with get, set
    [<Column("table_type")>]
    member val TableType = "" with get, set

    member val Schema = null : Schema with get, set

    member val Columns = null : List<Column> with get, set
    member val TableConstraints = null : List<TableConstraint> with get, set

[<Table("columns", Schema="information_schema")>]
and private Column () =
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

[<Table("table_constraints", Schema="information_schema")>]
and private TableConstraint () =
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

    member val KeyColumnUsage = null : List<KeyColumn> with get, set
    member val ConstraintColumnUsage = null : List<ConstraintColumn> with get, set

[<Table("key_column_usage", Schema="information_schema")>]
and private KeyColumn () =
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

[<Table("constraint_column_usage", Schema="information_schema")>]
and private ConstraintColumn () =
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

[<Table("sequences", Schema="information_schema")>]
and private Sequence () =
    [<Column("sequence_catalog")>]
    member val SequenceCatalog = "" with get, set
    [<Column("sequence_schema")>]
    member val SequenceSchema = "" with get, set
    [<Column("sequence_name")>]
    member val SequenceName = "" with get, set

    member val Schema = null : Schema with get, set
    
let private parseConstraintType (str : string) =
    match str.ToUpper() with
        | "UNIQUE" -> Some CTUnique
        | "CHECK" -> Some CTCheck
        | "PRIMARY KEY" -> Some CTPrimaryKey
        | "FOREIGN KEY" -> Some CTForeignKey
        | _ -> None

let private tryRegclass (str : string) =
    match parse tokenizeSQL schemaObject with
        | Ok obj -> Some obj
        | Err msg -> None

let private makeTableFromName (schema : string) (table : string) : TableRef =
    { schema = if schema = "public" then None else Some (SQLName schema)
      name = SQLName table
    }

let private makeColumnFromName (schema : string) (table : string) (column : string) : ResolvedColumnRef =
    { table = makeTableFromName schema table
      name = SQLName column
    }

// Convert string-cast patterns into actual types so that we reverse lost type information.
// Don't reduce the expressions beyond that!
let private normalizeDefaultExpr : ValueExpr -> ValueExpr =
    let traverse = function
        | VECast (VEValue (VString str), typ) as castExpr ->
            match typ with
                | VTArray scalarType ->
                    match parse tokenizeArray stringArray with
                        | Error msg -> raise <| SQLMetaError <| sprintf "Cannot parse array: %s" msg
                        | Ok array ->
                            let runArrayCast constrFunc castFunc =
                                let runCast (str : string) =
                                    match castFunc str with
                                        | Some v -> v
                                        | None -> raise <| SQLMetaError <| sprintf "Cannot cast array value to type %s: %s" scalarType str
                                VEValue <| constrFunc <| mapValueArray runCast array
                                
                            match findSimpleType scalarType with
                                | None -> castExpr
                                | Some STString -> VEValue <| VStringArray array
                                | Some STInt -> runArrayCast VIntArray tryIntInvariant
                                | Some STBool -> runArrayCast VBoolArray tryBool
                                | Some STDateTime -> runArrayCast VDateTimeArray tryDateTimeInvariant
                                | Some STDate -> runArrayCast VDateArray tryDateTimeInvariant
                                | Some STRegclass -> runArrayCast VRegclassArray tryRegclass
                | VTScalar scalarType ->
                    let runCast constrFunc castFunc =
                        match castFunc str with
                            | Some v -> VEValue <| constrFunc v
                            | None -> raise <| SQLMetaError <| sprintf "Cannot cast scalar value to type %s: %s" scalarType str

                    match findSimpleType scalarType with
                        | None -> castExpr
                        | Some STString -> castExpr
                        | Some STInt -> runCast VInt tryIntInvariant
                        | Some STBool -> runCast VBool tryBool
                        | Some STDateTime -> runCast VDateTime tryDateTimeInvariant
                        | Some STDate -> runCast VDate tryDateTimeInvariant
                        | Some STRegclass -> runCast VRegclass tryRegclass
        | VEValue value -> VEValue value
        | VEColumn c -> raise <| SQLMetaError <| sprintf "Invalid reference in default expression: %O" c
        | VEPlaceholder i -> raise <| SQLMetaError <| sprintf "Invalid placeholder in default expression: %i" i
        | VENot e -> VENot (traverse e)
        | VEAnd (a, b) -> VEAnd (traverse a, traverse b)
        | VEOr (a, b) -> VEOr (traverse a, traverse b)
        | VEConcat (a, b) -> VEConcat (traverse a, traverse b)
        | VEEq (a, b) -> VEEq (traverse a, traverse b)
        | VENotEq (a, b) -> VENotEq (traverse a, traverse b)
        | VELike (e, pat) -> VELike (traverse e, traverse pat)
        | VENotLike (e, pat) -> VENotLike (traverse e, traverse pat)
        | VELess (a, b) -> VELess (traverse a, traverse b)
        | VELessEq (a, b) -> VELessEq (traverse a, traverse b)
        | VEGreater (a, b) -> VEGreater (traverse a, traverse b)
        | VEGreaterEq (a, b) -> VEGreaterEq (traverse a, traverse b)
        | VEIn (e, vals) -> VEIn (traverse e, Array.map traverse vals)
        | VENotIn (e, vals) -> VENotIn (traverse e, Array.map traverse vals)
        | VEFunc (name,  args) -> VEFunc (name, Array.map traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
    traverse

let private makeColumnMeta (column : Column) : ColumnName * ColumnMeta =
    let columnType =
        if column.UdtSchema <> "pg_catalog" then
            raise <| SQLMetaError <| sprintf "Unsupported user-defined data type: %s.%s" column.UdtSchema column.UdtName
        match parseUdtName column.UdtName with
            | Some name -> name
            | None -> raise <| SQLMetaError <| sprintf "Unknown data type: %s" column.UdtName
    let defaultExpr =
        if column.ColumnDefault = null
        then None
        else
            match parse tokenizeSQL valueExpr column.ColumnDefault with
                | Ok expr -> normalizeDefaultExpr expr
                | Error msg -> raise <| SQLMetaError <| sprintf "Cannot parse column default value: %s" msg
    let isNullable =
        match tryBool column.IsNullable with
            | Some v -> v
            | None -> raise <| SQLMetaError <| sprintf "Invalid is_nullable value: %s" column.IsNullable
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
    match parseConstraintType constr.ConstraintType with
        | None -> raise <| SQLMetaError <| spritnf "Unknown constraint type: %s" constr.ConstraintType
        | Some CTUnique -> CMUnique <| keyColumns ()
        | Some CTPrimaryKey -> CMPrimaryKey <| keyColumns ()
        | Some CTCheck -> failwith "Not implemented yet"
        | Some CTForeignKey -> CMForeignKey <| Array.zip (keyColumns ()) (constraintColumns ())

let private makeTableMeta (table : Table) : TableName * TableMeta * (ConstraintName * ConstraintMeta) seq =
    if table.TableType.ToUpper() <> "BASE TABLE" then
        raise <| SQLMetaError <| sprintf "Unsupported table type: %s" table.TableType
    let constraints = table.TableConstraints |> Seq.map makeConstraintMeta
    let res = { columns = table.Columns |> Seq.map makeColumnMeta |> mapOfSeqUnique }
    (SQLName table.TableName, res, constraints)

let private makeSchemaMeta (schema : Schema) : SchemaName option * SchemaMeta =
    let tableObjects = schema.Tables |> Seq.map makeTableMeta |> Seq.cache
    let tables = tableObjects |> Seq.map (fun (name, (table, constraints)) -> (name, OMTable table)) |> mapOfSeqUnique
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
    use db = InformationSchemaContext dbOptions.Options

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
