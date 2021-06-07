using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql.NameTranslation;

using FunWithFlags.FunDBSchema.Attributes;

namespace FunWithFlags.FunDBSchema.System
{
    public class SystemContext : PostgresContext
    {
        [Entity("name", ForbidExternalReferences=true, ForbidTriggers=true, IsHidden=true)]
        [UniqueConstraint("name", new [] {"name"})]
        [CheckConstraint("not_empty", "name <> ''")]
        public DbSet<StateValue> State { get; set; } = null!;

        [Entity("name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [UniqueConstraint("name", new [] {"name"})]
        public DbSet<Schema> Schemas { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        public DbSet<Entity> Entities { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ColumnField> ColumnFields { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ComputedField> ComputedFields { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        [CheckConstraint("not_empty", "columns <> (array[] :: array(string))")]
        public DbSet<UniqueConstraint> UniqueConstraints { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        public DbSet<CheckConstraint> CheckConstraints { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        [CheckConstraint("not_empty", "expressions <> (array[] :: array(string))")]
        public DbSet<Index> Indexes { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<UserView> UserViews { get; set; } = null!;

        [Entity("name")]
        [UniqueConstraint("name", new [] {"name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<User> Users { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<Role> Roles { get; set; } = null!;

        [Entity("id", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [UniqueConstraint("role", new [] {"role_id", "parent_id"})]
        public DbSet<RoleParent> RoleParents { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "role_id=>__main || '.' || entity_id=>__main")]
        [UniqueConstraint("entry", new [] {"role_id", "entity_id"})]
        public DbSet<RoleEntity> RoleEntities { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "role_entity_id=>__main || '.' || column_name")]
        [UniqueConstraint("entry", new [] {"role_entity_id", "column_name"})]
        public DbSet<RoleColumnField> RoleColumnFields { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || field_entity_id=>__main || '.' || field_name")]
        [UniqueConstraint("entry", new [] {"schema_id", "field_entity_id", "field_name"})]
        public DbSet<FieldAttributes> FieldsAttributes { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || path")]
        [UniqueConstraint("entry", new [] {"schema_id", "path"})]
        public DbSet<Module> Modules { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("entry", new [] {"schema_id", "name"})]
        public DbSet<Action> Actions { get; set; } = null!;

        [Entity("full_name", ForbidExternalReferences=true, ForbidTriggers=true, TriggersMigration=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || trigger_entity_id=>__main || '.' || name")]
        [UniqueConstraint("entry", new [] {"schema_id", "trigger_entity_id", "name"})]
        public DbSet<Trigger> Triggers { get; set; } = null!;

        [Entity("id", ForbidTriggers=true, IsFrozen=true)]
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
                table.SetTableName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(table.GetTableName()));
                foreach (var property in table.GetProperties())
                {
                    var storeObjectId =
                        StoreObjectIdentifier.Create(property.DeclaringEntityType, StoreObjectType.Table)!.Value;
                    property.SetColumnName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(property.GetColumnName(storeObjectId)));
                }
            }
        }

        public IQueryable<Schema> GetLayoutObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.Entities).ThenInclude(ent => ent.ColumnFields)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.ComputedFields)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.UniqueConstraints)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.CheckConstraints)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.Indexes)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.Parent).ThenInclude(ent => ent!.Schema);
        }

        public IQueryable<Schema> GetRolesObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.Roles).ThenInclude(role => role.Parents).ThenInclude(roleParent => roleParent.Parent).ThenInclude(role => role!.Schema)
                .Include(sch => sch.Roles).ThenInclude(role => role.Entities).ThenInclude(roleEnt => roleEnt.Entity).ThenInclude(ent => ent!.Schema)
                .Include(sch => sch.Roles).ThenInclude(role => role.Entities).ThenInclude(roleEnt => roleEnt.ColumnFields);
        }

        public IQueryable<Schema> GetAttributesObjects()
        {
            return this.Schemas
                .AsSplitQuery()
                .Include(sch => sch.FieldsAttributes).ThenInclude(attr => attr.FieldEntity).ThenInclude(ent => ent!.Schema);
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
                .Include(sch => sch.Triggers).ThenInclude(attr => attr.TriggerEntity).ThenInclude(ent => ent!.Schema);
        }

        public IQueryable<Schema> GetUserViewsObjects()
        {
            return this.Schemas
                .AsSingleQuery()
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
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("string")]
        public string? UserViewGeneratorScript { get; set; }
        [ColumnField("bool", Default="false")]
        public bool UserViewGeneratorScriptAllowBroken { get; set; }

        public List<Entity>? Entities { get; set; }
        public List<Role>? Roles { get; set; }
        public List<FieldAttributes>? FieldsAttributes { get; set; }
        public List<Module>? Modules { get; set; }
        public List<Action>? Actions { get; set; }
        public List<Trigger>? Triggers { get; set; }
        public List<UserView>? UserViews { get; set; }
    }

    public class Entity
    {
        public int Id { get; set; }
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("reference(public.schemas)", IsImmutable=true)]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }
        [ColumnField("string")]
        public string? MainField { get; set; }
        [ColumnField("bool", Default="false")]
        public bool ForbidExternalReferences { get; set; }
        [ColumnField("bool", Default="false")]
        public bool IsAbstract { get; set; }

        [ColumnField("bool", Default="false")]
        public bool IsFrozen { get; set; }
        [ColumnField("reference(public.entities)", IsImmutable=true)]
        public int? ParentId { get; set; }
        public Entity? Parent { get; set; }

        public List<ColumnField> ColumnFields { get; set; } = null!;
        public List<ComputedField> ComputedFields { get; set; } = null!;
        public List<UniqueConstraint> UniqueConstraints { get; set; } = null!;
        public List<CheckConstraint> CheckConstraints { get; set; } = null!;
        public List<Index> Indexes { get; set; } = null!;
    }

    public class ColumnField
    {
        public int Id { get; set; }
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("reference(public.entities)", IsImmutable=true)]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Type { get; set; } = null!;
        [ColumnField("string")]
        public string? Default { get; set; }
        [ColumnField("bool", Default="false")]
        public bool IsNullable { get; set; }
        [ColumnField("bool", Default="false")]
        public bool IsImmutable { get; set; }
    }

    public class ComputedField
    {
        public int Id { get; set; }
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("reference(public.entities)", IsImmutable=true)]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }
        [ColumnField("bool", Default="false")]
        public bool IsVirtual { get; set; }

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
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }
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
        [ColumnField("reference(public.entities)")]
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
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }
        // Order is important here.
        [ColumnField("array(string)")]
        [Required]
        public string[] Expressions { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool IsUnique { get; set; }
        [ColumnField("string")]
        public string? Predicate { get; set; }
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
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }
        [ColumnField("string")]
        [Required]
        public string Query { get; set; } = null!;
    }

    public class User
    {
        public int Id { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool IsRoot { get; set; }
        [ColumnField("reference(public.roles)")]
        public int? RoleId { get; set; }
        public Role? Role { get; set; }
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
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }

        public List<RoleParent>? Parents { get; set; }
        public List<RoleParent>? Children { get; set; }
        public List<RoleEntity>? Entities { get; set; }
    }

    public class RoleParent
    {
        public int Id { get; set; }
        [ColumnField("reference(public.roles)")]
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
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity? Entity { get; set; }
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }
        [ColumnField("bool", Default="false")]
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
        [ColumnField("reference(public.role_entities)")]
        public int RoleEntityId { get; set; }
        public RoleEntity? RoleEntity { get; set; }
        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [ColumnField("string")]
        [Required]
        public string ColumnName { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool Change { get; set; }
        [ColumnField("string")]
        public string? Select { get; set; }
    }

    public class FieldAttributes
    {
        public int Id { get; set; }
        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema? Schema { get; set; }
        [ColumnField("reference(public.entities)")]
        public int FieldEntityId { get; set; }
        public Entity? FieldEntity { get; set; }
        [ColumnField("string")]
        [Required]
        public string FieldName { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }
        [ColumnField("int", Default="0")]
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
        [ColumnField("bool", Default="false")]
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
        [ColumnField("reference(public.entities)")]
        public int TriggerEntityId { get; set; }
        public Entity? TriggerEntity { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool AllowBroken { get; set; }
        [ColumnField("int", Default="0")]
        public int Priority { get; set; }
        [ColumnField("enum('BEFORE', 'AFTER')")]
        [Required]
        public string Time { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool OnInsert { get; set; }
        [ColumnField("array(string)", Default="array[]")]
        public string[] OnUpdateFields { get; set; } = null!;
        [ColumnField("bool", Default="false")]
        public bool OnDelete { get; set; }
        [ColumnField("string")]
        [Required]
        public string Procedure { get; set; } = null!;
    }

    public class EventEntry
    {
        public int Id { get; set; }

        [ColumnField("int", IsImmutable=true, Default="-1")]
        public int TransactionId { get; set; }
        [ColumnField("datetime", IsImmutable=true)]
        public DateTime TransactionTimestamp { get; set; }
        [ColumnField("datetime", IsImmutable=true)]
        public DateTime Timestamp { get; set; }
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Type { get; set; } = null!;
        [ColumnField("json", IsImmutable=true, Default="{type: 'api'}")]
        [Column(TypeName = "jsonb")]
        [Required]
        public string Source { get; set; } = "";
        [ColumnField("string", IsImmutable=true)]
        public string? UserName { get; set; }
        [ColumnField("string", IsImmutable=true)]
        public string? SchemaName { get; set; }
        [ColumnField("string", IsImmutable=true)]
        public string? EntityName { get; set; }
        [ColumnField("string", IsImmutable=true)]
        public string? FieldName { get; set; }
        [ColumnField("int", IsImmutable=true)]
        public int? EntityId { get; set; }
        [ColumnField("string", IsImmutable=true)]
        public string? Error { get; set; }
        [ColumnField("string", IsImmutable=true)]
        [Required]
        public string Details { get; set; } = "";
     }
}
