using System;
using System.Linq;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Npgsql.NameTranslation;

using FunWithFlags.FunDBSchema.Attributes;

namespace FunWithFlags.FunDBSchema.System
{
    public class SystemContext : DbContext
    {
        [Entity("name", ForbidExternalReferences=true, Hidden=true)]
        [UniqueConstraint("name", new [] {"name"})]
        [CheckConstraint("not_empty", "name <> ''")]
        public DbSet<StateValue> State { get; set; }

        [Entity("name", ForbidExternalReferences=true)]
        [UniqueConstraint("name", new [] {"name"})]
        public DbSet<Schema> Schemas { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        public DbSet<Entity> Entities { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ColumnField> ColumnFields { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> '' AND name <> 'id' AND name <> 'sub_entity'")]
        public DbSet<ComputedField> ComputedFields { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        [CheckConstraint("not_empty", "columns <> ([] :: array(string))")]
        public DbSet<UniqueConstraint> UniqueConstraints { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "entity_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"entity_id", "name"})]
        [CheckConstraint("not_reserved", "name NOT LIKE '%\\\\_\\\\_%' AND name <> ''")]
        public DbSet<CheckConstraint> CheckConstraints { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<UserView> UserViews { get; set; }

        [Entity("name")]
        [UniqueConstraint("name", new [] {"name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<User> Users { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || name")]
        [UniqueConstraint("name", new [] {"schema_id", "name"})]
        [CheckConstraint("not_reserved", "name <> ''")]
        public DbSet<Role> Roles { get; set; }

        [Entity("id", ForbidExternalReferences=true)]
        [UniqueConstraint("role", new [] {"role_id", "parent_id"})]
        public DbSet<RoleParent> RoleParents { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "role_id=>__main || '.' || entity_id=>__main")]
        [UniqueConstraint("entry", new [] {"role_id", "entity_id"})]
        public DbSet<RoleEntity> RoleEntities { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "role_entity_id=>__main || '.' || column_name")]
        [UniqueConstraint("entry", new [] {"role_entity_id", "column_name"})]
        public DbSet<RoleColumnField> RoleColumnFields { get; set; }

        [Entity("full_name", ForbidExternalReferences=true)]
        [ComputedField("full_name", "schema_id=>__main || '.' || field_entity_id=>__main || '.' || field_name")]
        [UniqueConstraint("entry", new [] {"schema_id", "field_entity_id", "field_name"})]
        public DbSet<FieldAttributes> FieldsAttributes { get; set; }

        [Entity("id")]
        public DbSet<EventEntry> Events { get; set; }

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
                .WithMany(role => role.Parents);
            modelBuilder.Entity<RoleParent>()
                .HasOne(roleParent => roleParent.Parent)
                .WithMany(role => role.Children);
            
            foreach (var table in modelBuilder.Model.GetEntityTypes())
            {
                table.SetTableName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(table.GetTableName()));
                foreach (var property in table.GetProperties())
                {
                    property.SetColumnName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(property.GetColumnName()));
                }
            }
        }

        private static IQueryable<Schema> IncludeFieldsObjects(IQueryable<Schema> schemas)
        {
            return schemas
                .Include(sch => sch.Entities).ThenInclude(ent => ent.ColumnFields);
        }

        public IQueryable<Schema> GetLayoutObjects()
        {
            return IncludeFieldsObjects(this.Schemas)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.ComputedFields)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.UniqueConstraints)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.CheckConstraints)
                .Include(sch => sch.Entities).ThenInclude(ent => ent.Parent).ThenInclude(ent => ent.Schema);
        }

        public IQueryable<Schema> GetRolesObjects()
        {
            return IncludeFieldsObjects(this.Schemas)
                .Include(sch => sch.Roles).ThenInclude(role => role.Parents).ThenInclude(roleParent => roleParent.Parent).ThenInclude(role => role.Schema)
                .Include(sch => sch.Roles).ThenInclude(role => role.Entities).ThenInclude(roleEnt => roleEnt.Entity).ThenInclude(ent => ent.Schema)
                .Include(sch => sch.Roles).ThenInclude(role => role.Entities).ThenInclude(roleEnt => roleEnt.ColumnFields);
        }

        public IQueryable<Schema> GetAttributesObjects()
        {
            return IncludeFieldsObjects(this.Schemas)
                .Include(sch => sch.FieldsAttributes).ThenInclude(attr => attr.FieldEntity).ThenInclude(ent => ent.Schema);
        }

        public IQueryable<Schema> GetUserViewsObjects()
        {
            return this.Schemas
                .Include(sch => sch.UserViews);
        }
    }

    public class StateValue
    {
        public int Id { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("string")]
        [Required]
        public string Value { get; set; }
    }

    public class Schema
    {
        public int Id { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Name { get; set; }

        public List<Entity> Entities { get; set; }
        public List<Role> Roles { get; set; }
        public List<FieldAttributes> FieldsAttributes { get; set; }
        public List<UserView> UserViews { get; set; }
    }

    public class Entity
    {
        public int Id { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Name { get; set; }
        [ColumnField("reference(public.schemas)", Immutable=true)]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("string")]
        public string MainField { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool ForbidExternalReferences { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Hidden { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool IsAbstract { get; set; }
        [ColumnField("reference(public.entities)", Immutable=true)]
        public int? ParentId { get; set; }
        public Entity Parent { get; set; }

        public List<ColumnField> ColumnFields { get; set; }
        public List<ComputedField> ComputedFields { get; set; }
        public List<UniqueConstraint> UniqueConstraints { get; set; }
        public List<CheckConstraint> CheckConstraints { get; set; }
    }

    public class ColumnField
    {
        public int Id { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Name { get; set; }
        [ColumnField("reference(public.entities)", Immutable=true)]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Type { get; set; }
        [ColumnField("string")]
        public string Default { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Nullable { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Immutable { get; set; }
    }

    public class ComputedField
    {
        public int Id { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Name { get; set; }
        [ColumnField("reference(public.entities)", Immutable=true)]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }

        [ColumnField("string")]
        [Required]
        public string Expression { get; set; }
    }

    public class UniqueConstraint
    {
        public int Id { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        // Order is important here
        // Change this if/when we implement "ordered 1-N references".
        [ColumnField("array(string)")]
        [Required]
        public string[] Columns { get; set; }
    }

    public class CheckConstraint
    {
        public int Id { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        [ColumnField("string")]
        [Required]
        public string Expression { get; set; }
    }

    public class UserView
    {
        public int Id { get; set; }
        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }
        [ColumnField("string")]
        [Required]
        public string Query { get; set; }
    }

    public class User
    {
        public int Id { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool IsRoot { get; set; }
        [ColumnField("reference(public.roles)")]
        public int? RoleId { get; set; }
        public Role Role { get; set; }
    }

    public class Role
    {
        public int Id { get; set; }
        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }

        public List<RoleParent> Parents { get; set; }
        public List<RoleParent> Children { get; set; }
        public List<RoleEntity> Entities { get; set; }
    }

    public class RoleParent
    {
        public int Id { get; set; }
        [ColumnField("reference(public.roles)")]
        public int RoleId { get; set; }
        public Role Role { get; set; }
        [ColumnField("reference(public.roles)")]
        public int ParentId { get; set; }
        public Role Parent { get; set; }
    }

    public class RoleEntity
    {
        public int Id { get; set; }
        [ColumnField("reference(public.roles)")]
        public int RoleId { get; set; }
        public Role Role { get; set; }
        [ColumnField("reference(public.entities)")]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Insert { get; set; }
        [ColumnField("string")]
        public string Check { get; set; }
        [ColumnField("string")]
        public string Select { get; set; }
        [ColumnField("string")]
        public string Update { get; set; }
        [ColumnField("string")]
        public string Delete { get; set; }

        public List<RoleColumnField> ColumnFields { get; set; }
    }

    public class RoleColumnField
    {
        public int Id { get; set; }
        [ColumnField("reference(public.role_entities)")]
        public int RoleEntityId { get; set; }
        public RoleEntity RoleEntity { get; set; }
        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [ColumnField("string")]
        [Required]
        public string ColumnName { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Change { get; set; }
        [ColumnField("string")]
        public string Select { get; set; }
    }

    public class FieldAttributes
    {
        public int Id { get; set; }
        [ColumnField("reference(public.schemas)")]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("reference(public.entities)")]
        public int FieldEntityId { get; set; }
        public Entity FieldEntity { get; set; }
        [ColumnField("string")]
        [Required]
        public string FieldName { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }
        [ColumnField("int", Default="0")]
        public int Priority { get; set; }
        [ColumnField("string")]
        [Required]
        public string Attributes { get; set; }
    }

    public class EventEntry
    {
        public int Id { get; set; }
        [ColumnField("datetime", Immutable=true)]
        public DateTime TransactionTimestamp { get; set; }
        [ColumnField("datetime", Immutable=true)]
        public DateTime Timestamp { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Type { get; set; }
        [ColumnField("string", Immutable=true)]
        public string UserName { get; set; }
        [ColumnField("string", Immutable=true)]
        public string SchemaName { get; set; }
        [ColumnField("string", Immutable=true)]
        public string EntityName { get; set; }
        [ColumnField("int", Immutable=true)]
        public int? EntityId { get; set; }
        [ColumnField("string", Immutable=true)]
        public string Error { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Details { get; set; }
     }
}
