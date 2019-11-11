using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

using FunWithFlags.FunDBSchema.Attributes;

namespace FunWithFlags.FunDBSchema.Schema
{
    public class SystemContext : DbContext
    {
        [Entity("Name", ForbidExternalReferences=true, Hidden=true)]
        [UniqueConstraint("Name", new [] {"Name"})]
        [CheckConstraint("NotEmpty", "\"Name\" <> ''")]
        public DbSet<StateValue> State { get; set; }

        [Entity("Name", ForbidExternalReferences=true)]
        [UniqueConstraint("Name", new [] {"Name"})]
        public DbSet<Schema> Schemas { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"SchemaId", "Name"})]
        public DbSet<Entity> Entities { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"EntityId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")]
        public DbSet<ColumnField> ColumnFields { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"EntityId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")]
        public DbSet<ComputedField> ComputedFields { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"EntityId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")]
        [CheckConstraint("NotEmpty", "\"Columns\" <> ([] :: array(string))")]
        public DbSet<UniqueConstraint> UniqueConstraints { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"EntityId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")]
        public DbSet<CheckConstraint> CheckConstraints { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"SchemaId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" <> ''")]
        public DbSet<UserView> UserViews { get; set; }

        [Entity("Name")]
        [UniqueConstraint("Name", new [] {"Name"})]
        [CheckConstraint("NotReserved", "\"Name\" <> ''")]
        public DbSet<User> Users { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")]
        [UniqueConstraint("Name", new [] {"SchemaId", "Name"})]
        [CheckConstraint("NotReserved", "\"Name\" <> ''")]
        public DbSet<Role> Roles { get; set; }

        [Entity("Id", ForbidExternalReferences=true)]
        [UniqueConstraint("Role", new [] {"RoleId", "ParentId"})]
        public DbSet<RoleParent> RoleParents { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"RoleId\"=>\"__main\" || '.' || \"EntityId\"=>\"__main\"")]
        [UniqueConstraint("Entry", new [] {"RoleId", "EntityId"})]
        public DbSet<RoleEntity> RoleEntities { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"RoleEntityId\"=>\"__main\" || '.' || \"ColumnName\"")]
        [UniqueConstraint("Entry", new [] {"RoleEntityId", "ColumnName"})]
        public DbSet<RoleColumnField> RoleColumnFields { get; set; }

        [Entity("FullName", ForbidExternalReferences=true)]
        [ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"FieldEntityId\"=>\"__main\" || '.' || \"FieldName\"")]
        [UniqueConstraint("Entry", new [] {"SchemaId", "FieldEntityId", "FieldName"})]
        public DbSet<FieldAttributes> FieldsAttributes { get; set; }

        [Entity("Id")]
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
        [ColumnField("reference(\"public\".\"Schemas\")", Immutable=true)]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("string", Nullable=true)]
        public string MainField { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool ForbidExternalReferences { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Hidden { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool IsAbstract { get; set; }
        [ColumnField("reference(\"public\".\"Entities\")", Nullable=true, Immutable=true)]
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
        [ColumnField("reference(\"public\".\"Entities\")", Immutable=true)]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }

        [ColumnField("string")]
        [Required]
        public string Type { get; set; }
        [ColumnField("string", Nullable=true)]
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
        [ColumnField("reference(\"public\".\"Entities\")", Immutable=true)]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }

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
        [ColumnField("reference(\"public\".\"Entities\")")]
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
        [ColumnField("reference(\"public\".\"Entities\")")]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        [ColumnField("string")]
        [Required]
        public string Expression { get; set; }
    }

    public class UserView
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"Schemas\")")]
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
        [ColumnField("reference(\"public\".\"Roles\")", Nullable=true)]
        public int? RoleId { get; set; }
        public Role Role { get; set; }
    }

    public class Role
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"Schemas\")")]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("string")]
        [Required]
        public string Name { get; set; }

        public List<RoleParent> Parents { get; set; }
        public List<RoleParent> Children { get; set; }
        public List<RoleEntity> Entities { get; set; }
    }

    public class RoleParent
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"Roles\")")]
        public int RoleId { get; set; }
        public Role Role { get; set; }
        [ColumnField("reference(\"public\".\"Roles\")")]
        public int ParentId { get; set; }
        public Role Parent { get; set; }
    }

    public class RoleEntity
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"Roles\")")]
        public int RoleId { get; set; }
        public Role Role { get; set; }
        [ColumnField("reference(\"public\".\"Entities\")")]
        public int EntityId { get; set; }
        public Entity Entity { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool AllowBroken { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Insert { get; set; }
        [ColumnField("string", Nullable=true)]
        public string Check { get; set; }
        [ColumnField("string", Nullable=true)]
        public string Select { get; set; }
        [ColumnField("string", Nullable=true)]
        public string Update { get; set; }
        [ColumnField("string", Nullable=true)]
        public string Delete { get; set; }

        public List<RoleColumnField> ColumnFields { get; set; }
    }

    public class RoleColumnField
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"RoleEntities\")")]
        public int RoleEntityId { get; set; }
        public RoleEntity RoleEntity { get; set; }
        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [ColumnField("string")]
        [Required]
        public string ColumnName { get; set; }
        [ColumnField("bool", Default="FALSE")]
        public bool Change { get; set; }
        [ColumnField("string", Nullable=true)]
        public string Select { get; set; }
    }

    public class FieldAttributes
    {
        public int Id { get; set; }
        [ColumnField("reference(\"public\".\"Schemas\")")]
        public int SchemaId { get; set; }
        public Schema Schema { get; set; }
        [ColumnField("reference(\"public\".\"Entities\")")]
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
        public DateTimeOffset TransactionTimestamp { get; set; }
        [ColumnField("datetime", Immutable=true)]
        public DateTimeOffset Timestamp { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Type { get; set; }
        [ColumnField("string", Nullable=true, Immutable=true)]
        public string UserName { get; set; }
        [ColumnField("string", Nullable=true, Immutable=true)]
        public string SchemaName { get; set; }
        [ColumnField("string", Nullable=true, Immutable=true)]
        public string EntityName { get; set; }
        [ColumnField("int", Nullable=true, Immutable=true)]
        public int? EntityId { get; set; }
        [ColumnField("string", Immutable=true)]
        [Required]
        public string Details { get; set; }
     }
}
