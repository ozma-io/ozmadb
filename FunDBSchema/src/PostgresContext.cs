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
        
        public static string PgGetExpr(string pg_node_tree, int relation_oid)
        {
            throw new InvalidOperationException();
        }
    
        public static string PgGetTriggerDef(int trigger_oid)
        {
            throw new InvalidOperationException();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            var pgGetExprMethod = typeof(PostgresContext).GetRuntimeMethod(nameof(PgGetExpr), new[] { typeof(string), typeof(int) });
            modelBuilder
                .HasDbFunction(pgGetExprMethod)
                .HasTranslation(args => SqlFunctionExpression.Create("pg_get_expr", args, typeof(string), null));

            var pgGetTriggerDefMethod = typeof(PostgresContext).GetRuntimeMethod(nameof(PgGetTriggerDef), new[] { typeof(int) });
            modelBuilder
                .HasDbFunction(pgGetTriggerDefMethod)
                .HasTranslation(args => SqlFunctionExpression.Create("pg_get_triggerdef", args, typeof(string), null));
        }
    }
}
