using System;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace FunWithFlags.FunDBSchema
{
    public abstract class PostgresContext : DbContext
    {
        public PostgresContext()
            : base()
        {
        }

        public PostgresContext(DbContextOptions options)
            : base(options)
        {
        }

        public static string PgGetExpr(string pgNodeTree, uint relationOid)
        {
            throw new InvalidOperationException();
        }

        public static string PgGetTriggerDef(uint triggerOid)
        {
            throw new InvalidOperationException();
        }

        public static bool PgIndexamHasProperty(uint amOid, string property)
        {
            throw new InvalidOperationException();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            var pgGetExprMethod = typeof(PostgresContext).GetRuntimeMethod(nameof(PgGetExpr), new[] { typeof(string), typeof(uint) })!;
            modelBuilder
                .HasDbFunction(pgGetExprMethod)
                .HasTranslation(args => new SqlFunctionExpression("pg_get_expr", args, false, new[] { true, true }, typeof(string), null));

            var pgGetTriggerDefMethod = typeof(PostgresContext).GetRuntimeMethod(nameof(PgGetTriggerDef), new[] { typeof(uint) })!;
            modelBuilder
                .HasDbFunction(pgGetTriggerDefMethod)
                .HasTranslation(args => new SqlFunctionExpression("pg_get_triggerdef", args, false, new[] { true }, typeof(string), null));

            var pgIndexamHasPropertyMethod = typeof(PostgresContext).GetRuntimeMethod(nameof(PgIndexamHasProperty), new[] { typeof(uint), typeof(string) })!;
            modelBuilder
                .HasDbFunction(pgIndexamHasPropertyMethod)
                .HasTranslation(args => new SqlFunctionExpression("pg_indexam_has_property", args, false, new[] { true, true }, typeof(bool), null));
        }
    }
}
