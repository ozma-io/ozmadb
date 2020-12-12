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
                .WithOne(def => def!.Attribute)
                .HasForeignKey<AttrDef>(def => new { def.AdRelId, def.AdNum });
            // Not really a string, just a dummy type
            modelBuilder.Entity<AttrDef>()
                .Property<string>("AdBin");

            modelBuilder.Entity<Constraint>()
                .Property<string>("ConBin");

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
            var ret = await this.Namespaces
                .AsNoTracking()
                .AsSplitQuery()
                .Include(ns => ns.Procs).ThenInclude(proc => proc.RetType)
                .Include(ns => ns.Procs).ThenInclude(proc => proc.Language)
                .Include(ns => ns.Classes).ThenInclude(cl => cl.Indexes).ThenInclude(cl => cl.Class)
                .Where(ns => !ns.NspName.StartsWith("pg_") && ns.NspName != "information_schema")
                .ToListAsync();

            IList<int> classesIds = ret.SelectMany(ns => ns.Classes).Select(cl => cl.Oid).ToList();

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
                .AsSplitQuery()
                .Include(constr => constr.FRelClass).ThenInclude(rcl => rcl.Namespace)
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
                .Where(trig => classesIds.Contains(trig.TgRelId) && !trig.TgIsInternal)
                .Include(trig => trig.Function).ThenInclude(func => func.Namespace)
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

            foreach (var ns in ret)
            {
                foreach (var cl in ns.Classes)
                {
                    var myAttrs = attrs.GetValueOrDefault(cl.Oid);
                    if (myAttrs == null)
                        myAttrs = new List<Attribute>();
                    cl.Attributes = myAttrs;

                    var myConstrs = constrs.GetValueOrDefault(cl.Oid);
                    if (myConstrs == null)
                        myConstrs = new List<Constraint>();
                    cl.Constraints = myConstrs;

                    var myTriggers = triggers.GetValueOrDefault(cl.Oid);
                    if (myTriggers == null)
                        myTriggers = new List<Trigger>();
                    cl.Triggers = myTriggers;
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

        public List<Class> Classes { get; set; } = null!;
        public List<Proc> Procs { get; set; } = null!;
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
        public Namespace Namespace { get; set; } = null!;

        public List<Attribute> Attributes { get; set; } = null!;
        [InverseProperty("RelClass")]
        public List<Constraint> Constraints { get; set; } = null!;
        [InverseProperty("RelClass")]
        public List<Index> Indexes { get; set; } = null!;
        [InverseProperty("RelClass")]
        public List<Trigger> Triggers { get; set; } = null!;
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
        public Class RelClass { get; set; } = null!;
        [ForeignKey("AttTypId")]
        public Type Type { get; set; } = null!;

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
        public string Source { get; set; } = null!;

        [ForeignKey("AdRelId")]
        public Class RelClass { get; set; } = null!;
        public Attribute Attribute { get; set; } = null!;
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
        public int? ConFRelId { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        public Int16[]? ConKey { get; set; }
        public Int16[]? ConFKey { get; set; }
        public bool ConDeferrable { get; set; }
        public bool ConDeferred { get; set; }

        [NotMapped]
        public string Source { get; set; } = null!;

        [ForeignKey("ConRelId")]
        public Class RelClass { get; set; } = null!;
        [ForeignKey("ConFRelId")]
        public Class FRelClass { get; set; } = null!;
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
        public Int16[] TgAttr { get; set; } = null!;
        [Required]
        public byte[] TgArgs { get; set; } = null!;

        [NotMapped]
        public string Source { get; set; } = null!;

        [ForeignKey("TgRelId")]
        public Class RelClass { get; set; } = null!;
        [ForeignKey("TgConstraint")]
        public Constraint Constraint { get; set; } = null!;
        [ForeignKey("TgFOid")]
        public Proc Function { get; set; } = null!;
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
        public Namespace Namespace { get; set; } = null!;
        [ForeignKey("ProLang")]
        public Language Language { get; set; } = null!;
        [ForeignKey("ProRetType")]
        public Type RetType { get; set; } = null!;
    }

    public class Index
    {
        [Column(TypeName="oid")]
        [Key]
        public int IndexRelId { get; set; }
        [Column(TypeName="oid")]
        public int IndRelId { get; set; }
        public bool IndIsUnique { get; set; }
        public bool IndIsPrimary { get; set; }
        public Int16[] IndKey { get; set; } = null!;

        [ForeignKey("IndexRelId")]
        public Class Class { get; set; } = null!;
        [ForeignKey("IndRelId")]
        public Class RelClass { get; set; } = null!;
    }
}
