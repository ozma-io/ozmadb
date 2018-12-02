module FunWithFlags.FunDB.SQL.Meta

open Microsoft.EntityFrameworkCore
open Microsoft.Extensions.Logging
open System.Linq

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Parsing
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Lexer
open FunWithFlags.FunDB.SQL.Parser
open FunWithFlags.FunDB.SQL.Array.Lexer
open FunWithFlags.FunDB.SQL.Array.Parser

exception SQLMetaError of info : string with
    override this.Message = this.info

module InformationSchema =
    open System.ComponentModel.DataAnnotations.Schema

    type InformationSchemaContext (options : DbContextOptions<InformationSchemaContext>) =
        inherit DbContext (options)

        // All of this shit is because of how EF Core works.
        [<DefaultValue>]
        val mutable schemata : DbSet<Schema>
        member this.Schemata 
            with get () = this.schemata 
            and set value = this.schemata <- value

        [<DefaultValue>]
        val mutable tables : DbSet<Table>
        member this.Tables 
            with get () = this.tables 
            and set value = this.tables <- value
 
        [<DefaultValue>]
        val mutable columns : DbSet<Column>
        member this.Columns
            with get () = this.columns
            and set value = this.columns <- value

        [<DefaultValue>]
        val mutable tableConstraints : DbSet<TableConstraint>
        member this.TableConstraints
            with get () = this.tableConstraints 
            and set value = this.tableConstraints <- value

        [<DefaultValue>]
        val mutable keyColumnUsage : DbSet<KeyColumn>
        member this.key_column_usage
            with get () = this.keyColumnUsage
            and set value = this.keyColumnUsage <- value

        [<DefaultValue>]
        val mutable constraintColumnUsage : DbSet<ConstraintColumn>
        member this.ConstraintColumnUsage
            with get () = this.constraintColumnUsage
            and set value = this.constraintColumnUsage <- value

        [<DefaultValue>]
        val mutable sequences : DbSet<Sequence>
        member this.Sequences
            with get () = this.sequences
            and set value = this.sequences <- value

        override this.OnModelCreating (modelBuilder : ModelBuilder) =
            ignore <| modelBuilder.Entity<Schema>()
                .HasKey([| "catalog_name"; "schema_name" |])
            ignore <| modelBuilder.Entity<Table>()
                .HasKey([| "table_catalog"; "table_schema"; "table_name" |])
            ignore <| modelBuilder.Entity<Table>()
                .HasOne(fun table -> table.schema)
                .WithMany(fun (schema : Schema) -> schema.tables)
                .HasForeignKey([| "table_catalog"; "table_schema" |])
            ignore <| modelBuilder.Entity<Column>()
                .HasKey([| "table_catalog"; "table_schema"; "table_name"; "column_name" |])
            ignore <| modelBuilder.Entity<Column>()
                .HasOne(fun column -> column.table)
                .WithMany(fun (table : Table) -> table.columns)
                .HasForeignKey([| "table_catalog"; "table_schema"; "table_name" |])
            ignore <| modelBuilder.Entity<TableConstraint>()
                .HasKey([| "constraint_catalog"; "constraint_schema"; "constraint_name" |])
            ignore <| modelBuilder.Entity<TableConstraint>()
                .HasOne(fun constr -> constr.table)
                .WithMany(fun (table : Table) -> table.table_constraints)
                .HasForeignKey([| "table_catalog"; "table_schema"; "table_name" |])
            ignore <| modelBuilder.Entity<KeyColumn>()
                .HasKey([| "constraint_catalog"; "constraint_schema"; "constraint_name"; "column_name" |])
            ignore <| modelBuilder.Entity<KeyColumn>()
                .HasOne(fun key -> key.table_constraint)
                .WithMany(fun (constr : TableConstraint) -> constr.key_column_usage)
                .HasForeignKey([| "constraint_catalog"; "constraint_schema"; "constraint_name" |])
            ignore <| modelBuilder.Entity<ConstraintColumn>()
                .HasKey([| "constraint_catalog"; "constraint_schema"; "constraint_name"; "table_catalog"; "table_schema"; "table_name"; "column_name" |])
            ignore <| modelBuilder.Entity<ConstraintColumn>()
                .HasOne(fun col -> col.table_constraint)
                .WithMany(fun (constr : TableConstraint) -> constr.constraint_column_usage)
                .HasForeignKey([| "constraint_catalog"; "constraint_schema"; "constraint_name" |])
            ignore <| modelBuilder.Entity<Sequence>()
                .HasKey([| "sequence_catalog"; "sequence_schema"; "sequence_name" |])
            ignore <| modelBuilder.Entity<Sequence>()
                .HasOne(fun seq -> seq.schema)
                .WithMany(fun (schema : Schema) -> schema.sequences)
                .HasForeignKey([| "sequence_catalog"; "sequence_schema" |])

    and
        [<Table("schemata", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        Schema =
            { catalog_name : string
              schema_name : string
              tables : Table seq
              sequences : Sequence seq
            }

    and
        [<Table("tables", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        Table =
            { table_catalog : string
              table_schema : string
              table_name : string
              table_type : string

              schema : Schema
        
              columns : Column seq
              table_constraints : TableConstraint seq
        }

    and
        [<Table("columns", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        Column =
            { table_catalog : string
              table_schema : string
              table_name : string
              column_name : string
              column_default : string
              is_nullable : string
              udt_catalog : string
              udt_schema : string
              udt_name : string

              table : Table
           }

    and
        [<Table("table_constraints", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        TableConstraint =
            { constraint_catalog : string
              constraint_schema : string
              constraint_name : string
              table_catalog : string
              table_schema : string
              table_name : string
              constraint_type : string

              table : Table

              key_column_usage : KeyColumn seq
              constraint_column_usage : ConstraintColumn seq
            }

    and
        [<Table("key_column_usage", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        KeyColumn =
            { constraint_catalog : string
              constraint_schema : string
              constraint_name : string
              table_catalog : string
              table_schema : string
              table_name : string
              column_name : string
              ordinal_position : int

              table_constraint : TableConstraint
            }

    and
        [<Table("constraint_column_usage", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        ConstraintColumn =
            { constraint_catalog : string
              constraint_schema : string
              constraint_name : string
              table_catalog : string
              table_schema : string
              table_name : string
              column_name : string

              table_constraint : TableConstraint
            }

    and
        [<Table("sequences", Schema="information_schema")>]
        [<CLIMutable>]
        [<NoEquality>]
        [<NoComparison>]
        Sequence =
            { sequence_catalog : string
              sequence_schema : string
              sequence_name : string

              schema : Schema
            }

 open InformationSchema
 open Npgsql

let private publicSchema = SQLName "public"

let private parseConstraintType (str : string) : ConstraintType option =
    match str.ToUpper() with
        | "UNIQUE" -> Some CTUnique
        | "CHECK" -> Some CTCheck
        | "PRIMARY KEY" -> Some CTPrimaryKey
        | "FOREIGN KEY" -> Some CTForeignKey
        | _ -> None

let private tryRegclass (str : string) : SchemaObject option =
    match parse tokenizeSQL schemaObject str with
        | Ok obj ->
            // "public" schema gets special handling because its mentions are omitted in PostgreSQL.
            let normalizedObj =
                if Option.isNone obj.schema then
                    { schema = Some publicSchema; name = obj.name }
                else
                    obj
            Some normalizedObj
        | Error msg -> None

let private makeTableFromName (schema : string) (table : string) : TableRef =
    { schema = Some (SQLName schema)
      name = SQLName table
    }

let private makeColumnFromName (schema : string) (table : string) (column : string) : ResolvedColumnRef =
    { table = makeTableFromName schema table
      name = SQLName column
    }

// Convert string-cast patterns into actual types so that we reverse lost type information.
// Don't reduce the expressions beyond that!
let private normalizeDefaultExpr : ValueExpr -> ValueExpr =
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
                                | Some STDateTime -> VEValue (VDateTimeArray <| runArrayCast tryDateTimeOffsetInvariant)
                                | Some STDate -> VEValue (VDateArray <| runArrayCast tryDateInvariant)
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
                        | Some STDateTime -> VEValue (VDateTime <| runCast tryDateTimeOffsetInvariant)
                        | Some STDate -> VEValue (VDate <| runCast tryDateInvariant)
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
        | VEInQuery (e, query) -> raise (SQLMetaError <| sprintf "Invalid subquery in default expression: %O" query)
        | VENotInQuery (e, query) -> raise (SQLMetaError <| sprintf "Invalid subquery in default expression: %O" query)
        | VEFunc (name,  args) -> VEFunc (name, Array.map traverse args)
        | VECast (e, typ) -> VECast (traverse e, typ)
        | VECase (es, els) -> VECase (Array.map (fun (cond, e) -> (traverse cond, traverse e)) es, Option.map traverse els)
    traverse

let parseUdtName (str : string) =
    if str.StartsWith("_") then
        VTArray <| SQLName (str.Substring(1))
    else
        VTScalar <| SQLName str

let private makeColumnMeta (column : Column) : ColumnName * ColumnMeta =
    let columnType =
        if column.udt_schema <> "pg_catalog" then
            raise (SQLMetaError <| sprintf "Unsupported user-defined data type: %s.%s" column.udt_schema column.udt_name)
        parseUdtName column.udt_name
    let defaultExpr =
        if column.column_default = null
        then None
        else
            match parse tokenizeSQL valueExpr column.column_default with
                | Ok expr -> Some <| normalizeDefaultExpr expr
                | Error msg -> raise (SQLMetaError <| sprintf "Cannot parse column default value: %s" msg)
    let isNullable =
        match tryBool column.is_nullable with
            | Some v -> v
            | None -> raise (SQLMetaError <| sprintf "Invalid is_nullable value: %s" column.is_nullable)
    let res =
        { columnType = columnType
          isNullable = isNullable
          defaultExpr = defaultExpr
        }
    (SQLName column.column_name, res)

let private makeConstraintMeta (constr : TableConstraint) : ConstraintName * ConstraintMeta =
    let keyColumns () =
        constr.key_column_usage |> Seq.sortBy (fun key -> key.ordinal_position) |> Seq.map (fun key -> SQLName key.column_name) |> Seq.toArray
    let constraintColumns () =
        // We don't have any key to sort on so we pray that PostgreSQL's result order is good. Re: move to pg_catalog.
        constr.constraint_column_usage |> Seq.map (fun col -> makeColumnFromName col.table_schema col.table_name col.column_name) |> Seq.toArray
    let res =
        match parseConstraintType constr.constraint_type with
            | None -> raise (SQLMetaError <| sprintf "Unknown constraint type: %s" constr.constraint_type)
            | Some CTUnique -> CMUnique (keyColumns ())
            | Some CTPrimaryKey -> CMPrimaryKey (keyColumns ())
            | Some CTCheck -> failwith "Not implemented yet"
            | Some CTForeignKey ->
                let cols = constraintColumns ()
                let checkTable (a : ResolvedColumnRef) (b : ResolvedColumnRef) =
                    if a.table <> b.table then
                        raise (SQLMetaError <| sprintf "Different column tables in a foreign key: %O vs %O" a.table b.table)
                    else b
                ignore <| Seq.fold1 checkTable cols
                let ref = cols.[0].table
                CMForeignKey (ref, Array.zip (keyColumns ()) (Array.map (fun (a : ResolvedColumnRef) -> a.name) cols))
    (SQLName constr.constraint_name, res)

let private makeTableMeta (table : Table) : TableName * TableMeta * (ConstraintName * ConstraintMeta) seq =
    if table.table_type.ToUpper() <> "BASE TABLE" then
        raise (SQLMetaError <| sprintf "Unsupported table type: %s" table.table_type)
    // FIXME: filtering CHECK constraints as we cannot handle them yet
    let constraints = table.table_constraints |> Seq.filter (fun x -> x.constraint_type <> "CHECK") |> Seq.map makeConstraintMeta
    let res = { columns = table.columns |> Seq.map makeColumnMeta |> Map.ofSeqUnique }
    (SQLName table.table_name, res, constraints)

let private makeSequenceMeta (sequence : Sequence) : TableName =
    SQLName sequence.sequence_name

let private makeSchemaMeta (schema : Schema) : SchemaName * SchemaMeta =
    let tableObjects = schema.tables |> Seq.map makeTableMeta |> Seq.cache
    let tables = tableObjects |> Seq.map (fun (name, table, constraints) -> (name, OMTable table)) |> Map.ofSeqUnique
    let constraints =
        tableObjects
        |> Seq.map (fun (name, table, constraints) -> Seq.map (fun (constrName, constr) -> (constrName, OMConstraint (name, constr))) constraints)
        |> Seq.concat
        |> Map.ofSeqUnique
    let sequences = schema.sequences |> Seq.map makeSequenceMeta |> Seq.map (fun name -> (name, OMSequence)) |> Map.ofSeqUnique
    let name = SQLName schema.schema_name
    let res = { objects = Map.unionUnique (Map.unionUnique tables constraints) sequences }
    (name, res)

let buildDatabaseMeta (transaction : NpgsqlTransaction) : DatabaseMeta =
    let dbOptions =
        (DbContextOptionsBuilder<InformationSchemaContext> ())
            .UseNpgsql(transaction.Connection)
    use db = new InformationSchemaContext(dbOptions.Options)
    ignore <| db.Database.UseTransaction(transaction)

    let systemSchemas = [| "information_schema"; "pg_catalog" |]
    let schemas =
        db.Schemata.Where(fun schema -> not (systemSchemas.Contains(schema.schema_name)))
            .Include(fun schema -> schema.tables)
                .ThenInclude(fun (table : Table) -> table.columns)
            .Include(fun schema -> schema.tables)
                .ThenInclude(fun (table : Table)-> table.table_constraints)
                    .ThenInclude(fun (constr : TableConstraint) -> constr.key_column_usage)
            .Include(fun schema -> schema.tables)
                .ThenInclude(fun (table : Table) -> table.table_constraints)
                    .ThenInclude(fun (constr : TableConstraint) -> constr.constraint_column_usage)
            .Include(fun schema -> schema.sequences)

    // We ignore catalog, assuming a database is constant -- it should be so according to PostgreSQL.
    { schemas = schemas |> Seq.map makeSchemaMeta |> Map.ofSeqUnique }