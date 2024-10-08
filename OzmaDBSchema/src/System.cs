using System;
using System.Linq;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql.NameTranslation;
using NodaTime;

using OzmaDBSchema.Attributes;

namespace OzmaDBSchema.System
{
    public class SystemContext : PostgresContext
    {
        [Entity("name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, IsHidden = true)]
        [UniqueConstraint("name", new[] { "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_empty", "name <> ''")]
        public DbSet<StateValue> State { get; set; } = null!;

        [Entity("name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [UniqueConstraint("name", new[] { "name" }, IsAlternateKey = true)]
        public DbSet<Schema> Schemas { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "schema_id", "name" }, IsAlternateKey = true)]
        public DbSet<Entity> Entities { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "entity_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ColumnField> ColumnFields { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "entity_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ComputedField> ComputedFields { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "entity_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        [CheckConstraint("not_empty", "columns <> (array[] :: array(string))")]
        public DbSet<UniqueConstraint> UniqueConstraints { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "entity_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        public DbSet<CheckConstraint> CheckConstraints { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "entity_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        [CheckConstraint("not_empty", "expressions <> (array[] :: array(string))")]
        public DbSet<Index> Indexes { get; set; } = null!;

        [Entity("name", SaveRestoreKey = "schema", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("name", "schema_id=>__main")]
        [UniqueConstraint("schema", new[] { "schema_id" }, IsAlternateKey = true)]
        public DbSet<UserViewGenerator> UserViewGenerators { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "schema_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<UserView> UserViews { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "schema_id", "name" }, IsAlternateKey = true)]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<Role> Roles { get; set; } = null!;

        [Entity("id", SaveRestoreKey = "entry", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [UniqueConstraint("entry", new[] { "role_id", "parent_id" }, IsAlternateKey = true)]
        public DbSet<RoleParent> RoleParents { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "entry", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "role_id=>__main || '.' || entity_id=>__main")]
        [UniqueConstraint("entry", new[] { "role_id", "entity_id" }, IsAlternateKey = true)]
        public DbSet<RoleEntity> RoleEntities { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "role_entity_id=>__main || '.' || column_name")]
        [UniqueConstraint("name", new[] { "role_entity_id", "column_name" }, IsAlternateKey = true)]
        public DbSet<RoleColumnField> RoleColumnFields { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || field_entity_id=>__main || '.' || field_name")]
        [UniqueConstraint("name", new[] { "schema_id", "field_entity_id", "field_name" }, IsAlternateKey = true)]
        public DbSet<FieldAttributes> FieldsAttributes { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "path", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || path")]
        [UniqueConstraint("path", new[] { "schema_id", "path" }, IsAlternateKey = true)]
        public DbSet<Module> Modules { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "schema_id", "name" }, IsAlternateKey = true)]
        public DbSet<Action> Actions { get; set; } = null!;

        [Entity("full_name", SaveRestoreKey = "name", InsertedInternally = true, UpdatedInternally = true, DeletedInternally = true, TriggersMigration = true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || trigger_entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new[] { "schema_id", "trigger_entity_id", "name" }, IsAlternateKey = true)]
        public DbSet<Trigger> Triggers { get; set; } = null!;

        [Entity("name")]
        [Attributes.Index("name", new[] { "lower(name)" }, IsUnique = true)]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<User> Users { get; set; } = null!;

        [Entity("id", InsertedInternally = true, IsFrozen = true)]

        [Attributes.Index("transaction_id", new[] { "\"transaction_id\"" })]
        [Attributes.Index("type", new[] { "\"type\"" })]
        [Attributes.Index("timestamp", new[] { "\"timestamp\"" })]
        [Attributes.Index("user_name", new[] { "\"user_name\"" })]
        [Attributes.Index(
            "request_user_view",
            new[] { "\"request\"->'source'->>'schema'", "\"request\"->'source'->>'name'", "\"timestamp\"" },
            Predicate = "\"request\"->'source'->>'schema' IS NOT NULL AND \"request\"->'source'->>'name' IS NOT NULL"
        )]
        [Attributes.Index(
            "request_entity_id",
            new[] { "\"request\"->'entity'->>'schema'", "\"request\"->'entity'->>'name'", "(\"details\"->>'id')::int", "\"timestamp\"" },
            Predicate = "\"request\"->'entity'->>'schema' IS NOT NULL AND \"request\"->'entity'->>'name' IS NOT NULL AND \"details\"->>'id' IS NOT NULL"
        )]
        [Attributes.Index(
            "request_entity",
            new[] { "\"request\"->'entity'->>'schema'", "\"request\"->'entity'->>'name'", "\"timestamp\"" },
            Predicate = "\"request\"->'entity'->>'schema' IS NOT NULL AND \"request\"->'entity'->>'name' IS NOT NULL")
        ]
        [Attributes.Index(
            "error_type",
            new[] { "\"error\"->>'error'" },
            Predicate = "\"error\"->>'error' IS NOT NULL"
        )]
        public DbSet<EventEntry> Events { get; set; } = null!;

        public SystemContext()
            : base()
        {
        }

        public SystemContext(DbContextOptions options)
            : base(options)
        {
        }

        override protected void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<RoleParent>()
                .HasOne(roleParent => roleParent.Role)
                .WithMany(role => role!.Parents);
            modelBuilder.Entity<RoleParent>()
                .HasOne(roleParent => roleParent.Parent)
                .WithMany(role => role!.Children);

            foreach (var table in modelBuilder.Model.GetEntityTypes())
            {
                table.SetTableName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(table.GetTableName()!));
                foreach (var property in table.GetProperties())
                {
                    var storeObjectId =
                        StoreObjectIdentifier.Create(property.DeclaringType, StoreObjectType.Table)!.Value;
                    property.SetColumnName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(property.GetColumnName(storeObjectId)!));
                }
            }
        }

        public IQueryable<Schema> GetLayoutObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.ColumnFields)
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.ComputedFields)
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.UniqueConstraints)
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.CheckConstraints)
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.Indexes)
                .Include(sch => sch.Entities!).ThenInclude(ent => ent.Parent!).ThenInclude(ent => ent.Schema);
        }

        public IQueryable<Schema> GetRolesObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.Roles!).ThenInclude(role => role.Parents!).ThenInclude(roleParent => roleParent.Parent!).ThenInclude(role => role.Schema)
                .Include(sch => sch.Roles!).ThenInclude(role => role.Entities!).ThenInclude(roleEnt => roleEnt.Entity!).ThenInclude(ent => ent.Schema)
                .Include(sch => sch.Roles!).ThenInclude(role => role.Entities!).ThenInclude(roleEnt => roleEnt.ColumnFields);
        }

        public IQueryable<Schema> GetAttributesObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.FieldsAttributes!).ThenInclude(attr => attr.FieldEntity!).ThenInclude(ent => ent.Schema);
        }

        public IQueryable<Schema> GetModulesObjects()
        {
            return this.Schemas
                .AsSingleQuery()
                .Include(sch => sch.Modules);
        }

        public IQueryable<Schema> GetActionsObjects()
        {
            return this.Schemas
                .AsSingleQuery()
                .Include(sch => sch.Actions);
        }

        public IQueryable<Schema> GetTriggersObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.Triggers!).ThenInclude(attr => attr.TriggerEntity!).ThenInclude(ent => ent.Schema);
        }

        public IQueryable<Schema> GetUserViewsObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.UserViewGenerator)
                .Include(sch => sch.UserViews);
        }
    }

    public class StateValue
    {
        public int Id { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string")]
        [Required]
        public string Value { get; set; } = null!;
    }

    public class Schema
    {
        public int Id { get; set; }

        [ColumnField("string", IsImmutable = true)]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        public List<Entity>? Entities { get; set; }
        public List<Role>? Roles { get; set; }
        public List<FieldAttributes>? FieldsAttributes { get; set; }
        public List<Module>? Modules { get; set; }
        public List<Action>? Actions { get; set; }
        public List<Trigger>? Triggers { get; set; }
        public UserViewGenerator? UserViewGenerator { get; set; }
        public List<UserView>? UserViews { get; set; }
    }

    public class Entity
    {
        public int Id { get; set; }

        [ColumnField("string", IsImmutable = true)]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.schemas)", IsImmutable = true)]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        public string? MainField { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsAbstract { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsFrozen { get; set; }

        [ColumnField("string")]
        public string? SaveRestoreKey { get; set; }

        [ColumnField("reference(public.entities)", IsImmutable = true)]
        public int? ParentId { get; set; }
        public Entity? Parent { get; set; }

        public List<ColumnField>? ColumnFields { get; set; }
        public List<ComputedField>? ComputedFields { get; set; }
        public List<UniqueConstraint>? UniqueConstraints { get; set; }
        public List<CheckConstraint>? CheckConstraints { get; set; }
        public List<Index>? Indexes { get; set; }
    }

    public class ColumnField
    {
        public int Id { get; set; }

        [ColumnField("string", IsImmutable = true)]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.entities) on delete cascade", IsImmutable = true)]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Type { get; set; } = null!;

        [ColumnField("string")]
        public string? Default { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsNullable { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsImmutable { get; set; }
    }

    public class ComputedField
    {
        public int Id { get; set; }

        [ColumnField("string", IsImmutable = true)]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.entities) on delete cascade", IsImmutable = true)]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsVirtual { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsMaterialized { get; set; }

        [ColumnField("string")]
        [Required]
        public string Expression { get; set; } = null!;
    }

    public class UniqueConstraint
    {
        public int Id { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.entities) on delete cascade")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool IsAlternateKey { get; set; }

        // Order is important here.
        [ColumnField("array(string)")]
        [Required]
        public string[] Columns { get; set; } = null!;
    }

    public class CheckConstraint
    {
        public int Id { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.entities) on delete cascade")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Expression { get; set; } = null!;
    }

    public class Index
    {
        public int Id { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("reference(public.entities) on delete cascade")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        // Order is important here.
        [ColumnField("array(string)")]
        [Required]
        public string[] Expressions { get; set; } = null!;

        [ColumnField("array(string)", Default = "array[]")]
        [Required]
        public string[] IncludedExpressions { get; set; } = new string[0];

        [ColumnField("bool", Default = "false")]
        public bool IsUnique { get; set; }

        [ColumnField("string")]
        public string? Predicate { get; set; }

        [ColumnField("enum('btree', 'gist', 'gin')", Default = "'btree'")]
        [Required]
        public string Type { get; set; } = "btree";
    }

    public class UserViewGenerator
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        [Required]
        public string Script { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }
    }

    public class UserView
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("string")]
        [Required]
        public string Query { get; set; } = null!;
    }

    public class Role
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        public List<RoleParent>? Parents { get; set; }
        public List<RoleParent>? Children { get; set; }
        public List<RoleEntity>? Entities { get; set; }
    }

    public class RoleParent
    {
        public int Id { get; set; }

        [ColumnField("reference(public.roles) on delete cascade")]
        public int RoleId { get; set; }
        public Role? Role { get; set; }

        [ColumnField("reference(public.roles)")]
        public int ParentId { get; set; }
        public Role? Parent { get; set; }
    }

    public class RoleEntity
    {
        public int Id { get; set; }

        [ColumnField("reference(public.roles)")]
        public int RoleId { get; set; }
        public Role? Role { get; set; }

        [ColumnField("reference(public.entities) on delete cascade")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("bool", Default = "false")]
        public bool Insert { get; set; }

        [ColumnField("string")]
        public string? Check { get; set; }

        [ColumnField("string")]
        public string? Select { get; set; }

        [ColumnField("string")]
        public string? Update { get; set; }

        [ColumnField("string")]
        public string? Delete { get; set; }

        public List<RoleColumnField>? ColumnFields { get; set; }
    }

    public class RoleColumnField
    {
        public int Id { get; set; }

        [ColumnField("reference(public.role_entities) on delete cascade")]
        public int RoleEntityId { get; set; }
        public RoleEntity? RoleEntity { get; set; }

        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [ColumnField("string")]
        [Required]
        public string ColumnName { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool Insert { get; set; }

        [ColumnField("string")]
        public string? Select { get; set; }

        [ColumnField("string")]
        public string? Update { get; set; }

        [ColumnField("string")]
        public string? Check { get; set; }
    }

    public class FieldAttributes
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("reference(public.entities) on delete cascade")]
        public int FieldEntityId { get; set; }
        public Entity? FieldEntity { get; set; }

        [ColumnField("string")]
        [Required]
        public string FieldName { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("int", Default = "0")]
        public int Priority { get; set; }

        [ColumnField("string")]
        [Required]
        public string Attributes { get; set; } = null!;
    }

    public class Module
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        [Required]
        public string Path { get; set; } = null!;

        [ColumnField("string")]
        [Required]
        public string Source { get; set; } = null!;
        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }
    }

    public class Action
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("string")]
        [Required]
        public string Function { get; set; } = null!;
    }

    public class Trigger
    {
        public int Id { get; set; }

        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }

        [ColumnField("reference(public.entities) on delete cascade")]
        public int TriggerEntityId { get; set; }
        public Entity? TriggerEntity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool AllowBroken { get; set; }

        [ColumnField("int", Default = "0")]
        public int Priority { get; set; }

        [ColumnField("enum('BEFORE', 'AFTER')")]
        [Required]
        public string Time { get; set; } = null!;

        [ColumnField("bool", Default = "false")]
        public bool OnInsert { get; set; }

        [ColumnField("array(string)", Default = "array[]")]
        public string[] OnUpdateFields { get; set; } = new string[0];

        [ColumnField("bool", Default = "false")]
        public bool OnDelete { get; set; }

        [ColumnField("string")]
        [Required]
        public string Procedure { get; set; } = null!;
    }

    public class User
    {
        public int Id { get; set; }

        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;

        [ColumnField("string", Default = "''")]
        [Required]
        public string Description { get; set; } = "";
        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Metadata { get; set; } = "{}";

        [ColumnField("bool", Default = "false")]
        public bool IsRoot { get; set; }

        [ColumnField("reference(public.roles) on delete set null")]
        public int? RoleId { get; set; }
        public Role? Role { get; set; }

        [ColumnField("bool", Default = "true")]
        public bool IsEnabled { get; set; } = true;
    }

    public class EventEntry
    {
        public int Id { get; set; }

        [ColumnField("int", IsImmutable = true, Default = "-1")]
        public int TransactionId { get; set; } = -1;

        [ColumnField("datetime", IsImmutable = true)]
        public Instant TransactionTimestamp { get; set; }

        [ColumnField("datetime", IsImmutable = true)]
        public Instant Timestamp { get; set; }

        [ColumnField("string", IsImmutable = true)]
        [Required]
        public string Type { get; set; } = null!;

        [ColumnField("string", IsImmutable = true)]
        public string? UserName { get; set; }

        [ColumnField("json", IsImmutable = true, Default = "{\"type\": 'api'}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Source { get; set; } = "{\"type\": \"api\"}";

        [ColumnField("json", IsImmutable = true)]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Request { get; set; } = null!;

        [ColumnField("json", IsImmutable = true)]
        [Column(TypeName = "jsonb")]
        public string? Response { get; set; }

        [ColumnField("json", Default = "{}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Details { get; set; } = "{}";

        [ColumnField("json", IsImmutable = true)]
        [Column(TypeName = "jsonb")]
        public string? Error { get; set; }
    }
}
