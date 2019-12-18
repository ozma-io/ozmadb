using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Npgsql.NameTranslation;

namespace FunWithFlags.FunDBSchema.Instances
{
    public class InstancesContext : DbContext
    {
        public DbSet<Instance> Instances { get; set; }

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
                table.SetTableName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(table.GetTableName()));
                foreach (var property in table.GetProperties())
                {
                    property.SetColumnName(NpgsqlSnakeCaseNameTranslator.ConvertToSnakeCase(property.GetColumnName()));
                }
            }
        }
    }

    public class Instance
    {
        public int Id { get; set; }
        [Required]
        public string Name { get; set; }
        [Required]
        public string ConnectionString { get; set; }
    }
}
