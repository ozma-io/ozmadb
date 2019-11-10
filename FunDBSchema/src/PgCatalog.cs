using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace FunWithFlags.FunDBSchema.PgCatalog
{
    public class PgCatalogContext : DbContext
    {
        // All of this shit is because of how EF Core works.
        public DbSet<Namespace> Namespaces { get; set; }
        public DbSet<Class> Classes { get; set; }
        public DbSet<Attribute> Attributes { get; set; }
        public DbSet<AttrDef> AttrDefs { get; set; }
        public DbSet<Constraint> Constraints { get; set; }

        public PgCatalogContext()
            : base()
        {
        }

        public PgCatalogContext(DbContextOptions options)
            : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Attribute>()
                .HasKey(new [] { "attrelid", "attnum" });
            modelBuilder.Entity<AttrDef>()
                .HasOne(def => def.attribute)
                .WithMany(attr => attr.attrDefs)
                .HasForeignKey(new [] { "adrelid", "adnum" });
        }
    }

    [Table("pg_namespace", Schema="pg_catalog")]
    public class Namespace
    {
        [Column(TypeName="oid")]
        [Key]
        public int oid { get; set; }
        [Required]
        public string nspname { get; set; }

        public List<Class> classes { get; set; }
    }

    [Table("pg_class", Schema="pg_catalog")]
    public class Class
    {
        [Column(TypeName="oid")]
        [Key]
        public int oid { get; set; }
        [Required]
        public string relname { get; set; }
        [Column(TypeName="oid")]
        public int relnamespace { get; set; }
        public char relkind { get; set; }

        [ForeignKey("relnamespace")]
        public Namespace pgNamespace { get; set; }

        public List<Attribute> attributes { get; set; }
        [InverseProperty("pgTableClass")]
        public List<Constraint> constraints { get; set; }
        [InverseProperty("pgTableClass")]
        public List<Index> indexes { get; set; }
    }

    [Table("pg_attribute", Schema="pg_catalog")]
    public class Attribute
    {
        [Column(TypeName="oid")]
        public int attrelid { get; set; }
        [Required]
        public string attname { get; set; }
        [Column(TypeName="oid")]
        public int atttypid { get; set; }
        public Int16 attnum { get; set; }
        public bool attnotnull { get; set; }
        public bool attisdropped { get; set; }

        [ForeignKey("attrelid")]
        public Class pgTableClass { get; set; }
        [ForeignKey("atttypid")]
        public Type pgType { get; set; }

        public List<AttrDef> attrDefs { get; set; }
    }

    [Table("pg_type", Schema="pg_catalog")]
    public class Type
    {
        [Column(TypeName="oid")]
        [Key]
        public int oid { get; set; }
        [Required]
        public string typname { get; set; }
        public char typtype { get; set; }
    }

    [Table("pg_attrdef", Schema="pg_catalog")]
    public class AttrDef
    {
        [Column(TypeName="oid")]
        [Key]
        public int oid { get; set; }
        [Column(TypeName="oid")]
        public int adrelid { get; set; }
        public Int16 adnum { get; set; }
        [Required]
        public string adsrc { get; set; }

        [ForeignKey("adrelid")]
        public Class pgTableClass { get; set; }
        public Attribute attribute { get; set; }
    }

    [Table("pg_constraint", Schema="pg_catalog")]
    public class Constraint
    {
        [Column(TypeName="oid")]
        [Key]
        public int oid { get; set; }
        public string conname { get; set; }
        public char contype { get; set; }
        [Column(TypeName="oid")]
        public int conrelid { get; set; }
        [Column(TypeName="oid")]
        public int? confrelid { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        public Int16[] conkey { get; set; }
        public Int16[] confkey { get; set; }
        public string consrc { get; set; }

        [ForeignKey("conrelid")]
        public Class pgTableClass { get; set; }
        [ForeignKey("confrelid")]
        public Class pgRelClass { get; set; }
    }

    [Table("pg_index", Schema="pg_catalog")]
    public class Index
    {
        [Column(TypeName="oid")]
        [Key]
        public int indexrelid { get; set; }
        [Column(TypeName="oid")]
        public int indrelid { get; set; }
        public bool indisunique { get; set; }
        public bool indisprimary { get; set; }
        public Int16[] indkey { get; set; }

        [ForeignKey("indexrelid")]
        public Class pgClass { get; set; }
        [ForeignKey("indrelid")]
        public Class pgTableClass { get; set; }
    }
}
