using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql.NameTranslation;
using NodaTime;

namespace OzmaDBSchema.Instances
{
    public class InstancesContext : DbContext
    {
        public DbSet<Instance> Instances { get; set; } = null!;

        public InstancesContext()
            : base()
        {
        }

        public InstancesContext(DbContextOptions options)
            : base(options)
        {
        }

        override protected void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

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
    }

    public class RateLimit
    {
        public int Period { get; set; } // secs
        public int Limit { get; set; }
    }

    public class Instance
    {
        public int Id { get; set; }
        [Required]
        public string Name { get; set; } = null!;

        public string Region { get; set; } = null!;
        [Required]
        public string Host { get; set; } = null!;
        public int Port { get; set; } = 5432;
        [Required]
        public string Username { get; set; } = null!;
        [Required]
        public string Password { get; set; } = null!;
        [Required]
        public string Database { get; set; } = null!;

        [Required]
        public string Owner { get; set; } = null!;
        public bool Enabled { get; set; } = true;
        public bool Published { get; set; } = true;
        public bool DisableSecurity { get; set; } = false;
        public bool AnyoneCanRead { get; set; } = false;
        [Required]
        public string[] ShadowAdmins { get; set; } = null!;

        public Instant? AccessedAt { get; set; }

        public int? MaxSize { get; set; }
        public int? MaxUsers { get; set; }
        public Duration? MaxRequestTime { get; set; }
        [Column(TypeName = "jsonb")]
        public List<RateLimit>? ReadRateLimitsPerUser { get; set; }
        [Column(TypeName = "jsonb")]
        public List<RateLimit>? WriteRateLimitsPerUser { get; set; }
    }
}
