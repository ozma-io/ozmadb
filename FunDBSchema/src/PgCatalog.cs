using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace FunWithFlags.FunDBSchema.PgCatalog
{
    public class PgCatalogContext : PostgresContext
    {
        public DbSet<Namespace> Namespaces { get; set; } = null!;
        public DbSet<Class> Classes { get; set; } = null!;
        public DbSet<Attribute> Attributes { get; set; } = null!;
        public DbSet<AttrDef> AttrDefs { get; set; } = null!;
        public DbSet<Constraint> Constraints { get; set; } = null!;
        public DbSet<Trigger> Triggers { get; set; } = null!;
        public DbSet<Depend> Depends { get; set; } = null!;
        public DbSet<Index> Index { get; set; } = null!;

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
            base.OnModelCreating(modelBuilder);

            modelBuilder.HasDefaultSchema("pg_catalog");

            modelBuilder.Entity<Attribute>()
                .HasKey(attr => new { attr.AttRelId, attr.AttNum });

            modelBuilder.Entity<Attribute>()
                .HasOne(attr => attr.AttrDef)
                .WithOne(def => def!.Attribute!)
                .HasForeignKey<AttrDef>(def => new { def.AdRelId, def.AdNum });
            // Not really a string, just a dummy type
            modelBuilder.Entity<AttrDef>()
                .Property<string>("AdBin");

            modelBuilder.Entity<Constraint>()
                .Property<string>("ConBin");

            modelBuilder.Entity<Depend>()
                .HasNoKey();

            modelBuilder.Entity<Index>()
                .Property<string[]>("IndExprs");

            foreach (var table in modelBuilder.Model.GetEntityTypes())
            {
                // Needs to be idempotent.
                var tableName = table.ClrType.Name.ToLower();
                if (!tableName.StartsWith("pg_"))
                {
                    tableName = "pg_" + tableName;
                }
                table.SetTableName(tableName);
                foreach (var property in table.GetProperties())
                {
                    var storeObjectId =
                        StoreObjectIdentifier.Create(property.DeclaringEntityType, StoreObjectType.Table)!.Value;
                    property.SetColumnName(property.GetColumnName(storeObjectId).ToLower());
                }
            }
        }

        public async Task<IEnumerable<Namespace>> GetObjects(CancellationToken cancellationToken)
        {
            // All this circus because just running:
            //
            // Include(x => x.foos).Select(x => new { X = x; Foos = x.foos.Select(..).ToList(); })
            //
            // Makes EFCore get Foos two times! This leads to a cartesian explosion quickly.
            // Instead we avoid `Include` and manually merge related entries.
            var retQuery = this.Namespaces
                .AsQueryable()
                .Where(ns => !ns.NspName.StartsWith("pg_") && ns.NspName != "information_schema");

            var ret = await retQuery
                .AsNoTracking()
                .AsSplitQuery()
                .Include(ns => ns.Classes)
                .Include(ns => ns.Procs).ThenInclude(proc => proc.RetType)
                .Include(ns => ns.Procs).ThenInclude(proc => proc.Language)
                .ToListAsync();

            var classesIds = retQuery.SelectMany(ns => ns.Classes).Select(cl => cl.Oid);

            var attrsList = await this.Attributes
                .AsNoTracking()
                .AsSplitQuery()
                .Include(attr => attr.Type)
                .Include(attr => attr.AttrDef)
                .Where(attr => classesIds.Contains(attr.AttRelId) && attr.AttNum > 0 && !attr.AttIsDropped)
                .Select(attr => new
                    {
                        Attribute = attr,
                        Source = PgGetExpr(EF.Property<string>(attr.AttrDef, "AdBin"), attr.AttrDef!.AdRelId),
                    })
                .ToListAsync();
            var attrs = attrsList
                .Select(attr =>
                {
                    if (attr.Attribute.AttrDef != null)
                        attr.Attribute.AttrDef.Source = attr.Source;
                    return attr.Attribute;
                })
                .GroupBy(attr => attr.AttRelId)
                .ToDictionary(g => g.Key, g => g.ToList());

            var constrsList = await this.Constraints
                .AsNoTracking()
                .Include(constr => constr.FRelClass).ThenInclude(rcl => rcl!.Namespace)
                .Where(constr => classesIds.Contains(constr.ConRelId))
                .Select(constr => new
                    {
                        Constraint = constr,
                        Source = PgGetExpr(EF.Property<string>(constr, "ConBin"), constr.ConRelId),
                    })
                .ToListAsync();
            var constrs = constrsList
                .Select(constr =>
                {
                    constr.Constraint.Source = constr.Source;
                    return constr.Constraint;
                })
                .GroupBy(constr => constr.ConRelId)
                .ToDictionary(g => g.Key, g => g.ToList());

            var triggersList = await this.Triggers
                .AsNoTracking()
                .Include(trig => trig.Function).ThenInclude(func => func!.Namespace)
                .Where(trig => classesIds.Contains(trig.TgRelId) && !trig.TgIsInternal)
                .Select(trig => new
                    {
                        Trigger = trig,
                        Source = PgGetTriggerDef(trig.Oid),
                    })
                .ToListAsync();
            var triggers = triggersList
                .Select(trig =>
                {
                        trig.Trigger.Source = trig.Source;
                        return trig.Trigger;
                })
                .GroupBy(trig => trig.TgRelId)
                .ToDictionary(g => g.Key, g => g.ToList());

            var indexesList = await this.Index
                .AsNoTracking()
                .Include(index => index.Class)
                .Where(index =>
                    classesIds.Contains(index.IndRelId) &&
                    !this.Constraints.AsQueryable().Where(constr => constr.ConIndId == index.IndexRelId).Any())
                .Select(index => new
                    {
                        Index = index,
                        Source = PgGetExpr(EF.Property<string>(index, "IndExprs"), index.IndexRelId),
                    })
                .ToListAsync();
            var indexes = indexesList
                .Select(index =>
                {
                    index.Index.Source = index.Source;
                    return index.Index;
                })
                .GroupBy(index => index.IndRelId)
                .ToDictionary(g => g.Key, g => g.ToList());

            foreach (var ns in ret)
            {
                foreach (var cl in ns.Classes!)
                {
                    cl.Attributes = attrs.GetValueOrDefault(cl.Oid);
                    cl.Constraints = constrs.GetValueOrDefault(cl.Oid);
                    cl.Triggers = triggers.GetValueOrDefault(cl.Oid);
                    cl.Indexes = indexes.GetValueOrDefault(cl.Oid);
                }
            }
            return ret;
        }
    }

    public class Namespace
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string NspName { get; set; } = null!;

        public List<Class>? Classes { get; set; }
        public List<Proc>? Procs { get; set; }
    }

    public class Class
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string RelName { get; set; } = null!;
        [Column(TypeName="oid")]
        public int RelNamespace { get; set; }
        public char RelKind { get; set; }

        [ForeignKey("RelNamespace")]
        public Namespace? Namespace { get; set; }

        public List<Attribute>? Attributes { get; set; }
        [InverseProperty("RelClass")]
        public List<Constraint>? Constraints { get; set; }
        [InverseProperty("RelClass")]
        public List<Index>? Indexes { get; set; }
        [InverseProperty("RelClass")]
        public List<Trigger>? Triggers { get; set; }
    }

    public class Attribute
    {
        [Column(TypeName="oid")]
        public int AttRelId { get; set; }
        [Required]
        public string AttName { get; set; } = null!;
        [Column(TypeName="oid")]
        public int AttTypId { get; set; }
        public Int16 AttNum { get; set; }
        public bool AttNotNull { get; set; }
        public bool AttIsDropped { get; set; }

        [ForeignKey("AttRelId")]
        public Class? RelClass { get; set; }
        [ForeignKey("AttTypId")]
        public Type? Type { get; set; }

        public AttrDef? AttrDef { get; set; }
    }

    public class Type
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string TypName { get; set; } = null!;
        public char TypType { get; set; }
    }

    public class Language
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string LanName { get; set; } = null!;
    }

    public class AttrDef
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Column(TypeName="oid")]
        public int AdRelId { get; set; }
        public Int16 AdNum { get; set; }

        [NotMapped]
        public string? Source { get; set; }

        [ForeignKey("AdRelId")]
        public Class? RelClass { get; set; }
        public Attribute? Attribute { get; set; }
    }

    public class Constraint
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string ConName { get; set; } = null!;
        public char ConType { get; set; }
        [Column(TypeName="oid")]
        public int ConRelId { get; set; }
        [Column(TypeName="oid")]
        public int? ConIndId { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        [Column(TypeName="oid")]
        public int? ConFRelId { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        public Int16[]? ConKey { get; set; }
        public Int16[]? ConFKey { get; set; }
        public bool ConDeferrable { get; set; }
        public bool ConDeferred { get; set; }

        [NotMapped]
        public string? Source { get; set; }

        [ForeignKey("ConRelId")]
        public Class? RelClass { get; set; }
        [ForeignKey("ConFRelId")]
        public Class? FRelClass { get; set; }
    }

    public class Trigger
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Column(TypeName="oid")]
        public int TgRelId { get; set; }
        [Required]
        public string TgName { get; set; } = null!;
        [Column(TypeName="oid")]
        public int TgFOid { get; set; }
        public bool TgIsInternal { get; set; }
        public Int16 TgType { get; set; }
        [Column(TypeName="oid")]
        public int? TgConstraint { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN
        [Required]
        public Int16[] TgAttr { get; set; } = null!;
        [Required]
        public byte[] TgArgs { get; set; } = null!;

        [NotMapped]
        public string? Source { get; set; }

        [ForeignKey("TgRelId")]
        public Class? RelClass { get; set; }
        [ForeignKey("TgConstraint")]
        public Constraint? Constraint { get; set; }
        [ForeignKey("TgFOid")]
        public Proc? Function { get; set; }
    }

    public class Proc
    {
        [Column(TypeName="oid")]
        [Key]
        public int Oid { get; set; }
        [Required]
        public string ProName { get; set; } = null!;
        [Column(TypeName="oid")]
        public int ProNamespace { get; set; }
        [Column(TypeName="oid")]
        public int ProLang { get; set; }
        public Int16 ProNArgs { get; set; }
        [Column(TypeName="oid")]
        public int ProRetType { get; set; }
        public char ProVolatile { get; set; }
        public bool ProRetSet { get; set; }
        [Required]
        public string ProSrc { get; set; } = null!;

        [ForeignKey("ProNamespace")]
        public Namespace? Namespace { get; set; }
        [ForeignKey("ProLang")]
        public Language? Language { get; set; }
        [ForeignKey("ProRetType")]
        public Type? RetType { get; set; }
    }

    public class Index
    {
        [Column(TypeName="oid")]
        [Key]
        public int IndexRelId { get; set; }
        [Column(TypeName="oid")]
        public int IndRelId { get; set; }
        public bool IndIsUnique { get; set; }
        [Required]
        public Int16[] IndKey { get; set; } = null!;

        [ForeignKey("IndexRelId")]
        public Class? Class { get; set; }
        [ForeignKey("IndRelId")]
        public Class? RelClass { get; set; }

        [NotMapped]
        public string? Source { get; set; }
    }

    public class Depend
    {
        [Column(TypeName="oid")]
        public int ClassId { get; set; }
        [Column(TypeName="oid")]
        public int ObjId { get; set; }
        public int ObjSubId { get; set; }
        [Column(TypeName="oid")]
        public int RefClassId { get; set; }
        [Column(TypeName="oid")]
        public int RefObjId { get; set; }
        public int RefObjSubId { get; set; }
        public char DepType { get; set; }
    }
}
