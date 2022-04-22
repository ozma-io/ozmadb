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
        public DbSet<Proc> Procs { get; set; } = null!;
        public DbSet<Extension> Extensions { get; set; } = null!;
        public DbSet<OpClass> OpClasses { get; set; } = null!;
        public DbSet<Am> Ams { get; set; } = null!;

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
            modelBuilder.Entity<Index>()
                .Property<string>("IndPred");

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
                    property.SetColumnName(property.GetColumnName(storeObjectId)!.ToLower());
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
                .Include(ns => ns.Extensions)
                .ToListAsync();

            var extDependIds = this.Depends.AsQueryable().Where(dep => this.Extensions.AsQueryable().Select(ext => ext.Oid).Contains(dep.RefObjId)).Select(dep => dep.ObjId);
            var namespaceIds = retQuery.Select(ns => ns.Oid);
            var classesIds = retQuery.SelectMany(ns => ns.Classes!).Select(cl => cl.Oid);

            var procsList = await this.Procs
                .AsNoTracking()
                .AsSplitQuery()
                .Include(proc => proc.RetType)
                .Include(proc => proc.Language)
                .Where(proc => namespaceIds.Contains(proc.ProNamespace) && !extDependIds.Contains(proc.Oid))
                .ToListAsync();
            var procs = procsList
                .GroupBy(proc => proc.ProNamespace)
                .ToDictionary(proc => proc.Key, proc => proc.ToList());

            var attrsList = await this.Attributes
                .AsNoTracking()
                .AsSplitQuery()
                .Include(attr => attr.Type)
                .Include(attr => attr.AttrDef)
                .Where(attr => classesIds.Contains(attr.AttRelId) && attr.AttNum > 0 && !attr.AttIsDropped)
                .Select(attr => new
                    {
                        Attribute = attr,
                        Source = PgGetExpr(EF.Property<string>(attr.AttrDef!, "AdBin"), attr.AttrDef!.AdRelId),
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
                // Could in principle build constraints map instead and get constraints by ids. Too lazy for now.
                .Include(trig => trig.Constraint)
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

            var opClasses = await this.OpClasses
                .AsNoTracking()
                .Where(opc => !opc.OpcDefault)
                .ToDictionaryAsync(opc => opc.Oid, opc => opc.OpcName);

            var indexesList = await this.Index
                .AsNoTracking()
                .Include(index => index.Class)
                .ThenInclude(cl => cl!.Am)
                .Where(index =>
                    classesIds.Contains(index.IndRelId) &&
                    !this.Constraints.AsQueryable().Where(constr => constr.ConIndId == index.IndexRelId).Any())
                .Select(index => new
                    {
                        Index = index,
                        ExprsSource = PgGetExpr(EF.Property<string>(index, "IndExprs"), index.IndRelId),
                        PredSource = PgGetExpr(EF.Property<string>(index, "IndPred"), index.IndRelId),
                        AmCanOrder = PgIndexamHasProperty(index.Class!.Am!.Oid, "can_order"),
                    })
                .ToListAsync();
            var indexes = indexesList
                .Select(index =>
                {
                    index.Index.ExprsSource = index.ExprsSource;
                    index.Index.PredSource = index.PredSource;
                    index.Index.Classes = index.Index.IndClass.Select(cl => opClasses.GetValueOrDefault((uint)cl)).ToArray();
                    index.Index.Class!.Am!.CanOrder = index.AmCanOrder;
                    return index.Index;
                })
                .GroupBy(index => index.IndRelId)
                .ToDictionary(g => g.Key, g => g.ToList());

            foreach (var ns in ret)
            {
                ns.Procs = procs.GetValueOrDefault(ns.Oid);
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
        public uint Oid { get; set; }
        [Required]
        public string NspName { get; set; } = null!;

        public List<Class>? Classes { get; set; }
        public List<Proc>? Procs { get; set; }
        public List<Extension>? Extensions { get; set; }
        public List<OpClass>? OpClasses { get; set; }
    }

    public class Class
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }
        [Required]
        public string RelName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint RelNamespace { get; set; }
        public char RelKind { get; set; }
        [Column(TypeName="oid")]
        public uint? RelAm { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.

        [ForeignKey("RelNamespace")]
        public Namespace? Namespace { get; set; }
        [ForeignKey("RelAm")]
        public Am? Am { get; set; }

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
        public uint AttRelId { get; set; }
        [Required]
        public string AttName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint AttTypId { get; set; }
        public Int16 AttNum { get; set; }
        public bool AttNotNull { get; set; }
        public bool AttIsDropped { get; set; }
        public char AttGenerated { get; set; }

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
        public uint Oid { get; set; }
        [Required]
        public string TypName { get; set; } = null!;
        public char TypType { get; set; }
    }

    public class Language
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }
        [Required]
        public string LanName { get; set; } = null!;
    }

    public class AttrDef
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }
        [Column(TypeName="oid")]
        public uint AdRelId { get; set; }
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
        public uint Oid { get; set; }
        [Required]
        public string ConName { get; set; } = null!;
        public char ConType { get; set; }
        [Column(TypeName="oid")]
        public uint ConRelId { get; set; }
        [Column(TypeName="oid")]
        public uint? ConIndId { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        [Column(TypeName="oid")]
        public uint? ConFRelId { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN for confrelid.
        public Int16[]? ConKey { get; set; }
        public Int16[]? ConFKey { get; set; }
        public bool ConDeferrable { get; set; }
        public bool ConDeferred { get; set; }
        public char? ConFUpdType { get; set; }
        public char? ConFDelType { get; set; }

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
        public uint Oid { get; set; }
        [Column(TypeName="oid")]
        public uint TgRelId { get; set; }
        [Required]
        public string TgName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint TgFOid { get; set; }
        public bool TgIsInternal { get; set; }
        public Int16 TgType { get; set; }
        [Column(TypeName="oid")]
        public uint? TgConstraint { get; set; } // Trick to make EFCore generate LEFT JOIN instead of INNER JOIN
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
        public uint Oid { get; set; }
        [Required]
        public string ProName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint ProNamespace { get; set; }
        [Column(TypeName="oid")]
        public uint ProLang { get; set; }
        public Int16 ProNArgs { get; set; }
        [Column(TypeName="oid")]
        public uint ProRetType { get; set; }
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

    [Flags]
    public enum IndexOptions : short
    {
        Desc = 1 << 0,
        NullsFirst = 1 << 1,
    }

    public class Index
    {
        [Column(TypeName="oid")]
        [Key]
        public uint IndexRelId { get; set; }
        [Column(TypeName="oid")]
        public uint IndRelId { get; set; }
        public Int16 IndNKeyAtts { get; set;}
        public bool IndIsUnique { get; set; }
        [Required]
        public Int16[] IndKey { get; set; } = null!;
        [Required]
        [Column(TypeName="oid[]")]
        public uint[] IndClass { get; set; } = null!;
        [Required]
        [Column(TypeName="int2[]")]
        public IndexOptions[] IndOption { get; set; } = null!;

        [ForeignKey("IndexRelId")]
        public Class? Class { get; set; }
        [ForeignKey("IndRelId")]
        public Class? RelClass { get; set; }

        [NotMapped]
        public string? ExprsSource { get; set; }
        [NotMapped]
        public string? PredSource { get; set; }
        [NotMapped]
        public string?[]? Classes { get; set; }
    }

    public class Depend
    {
        [Column(TypeName="oid")]
        public uint ClassId { get; set; }
        [Column(TypeName="oid")]
        public uint ObjId { get; set; }
        public int ObjSubId { get; set; }
        [Column(TypeName="oid")]
        public uint RefClassId { get; set; }
        [Column(TypeName="oid")]
        public uint RefObjId { get; set; }
        public int RefObjSubId { get; set; }
        public char DepType { get; set; }
    }

    public class Extension
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }

        [Required]
        public string ExtName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint ExtNamespace { get; set; }
        public bool ExtRelocatable { get; set; }

        [ForeignKey("ExtNamespace")]
        public Namespace? Namespace { get; set; }
    }

    public class OpClass
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }

        [Required]
        public string OpcName { get; set; } = null!;
        [Column(TypeName="oid")]
        public uint OpcNamespace { get; set; }
        public bool OpcDefault { get; set; }

        [ForeignKey("OpcNamespace")]
        public Namespace? Namespace { get; set; }
    }

    public class Am
    {
        [Column(TypeName="oid")]
        [Key]
        public uint Oid { get; set; }

        [Required]
        public string AmName { get; set; } = null!;

        [NotMapped]
        public bool? CanOrder { get; set; }
    }
}
