#pragma warning disable NPG9001

using System;
using Npgsql.Internal;
using Npgsql.Internal.Postgres;

namespace OzmaDBSchema
{
    public class ExtraTypeInfoResolverFactory : PgTypeInfoResolverFactory
    {
        public override IPgTypeInfoResolver CreateResolver() => new Resolver();
        public override IPgTypeInfoResolver? CreateArrayResolver() => new ArrayResolver();

        class Resolver : IPgTypeInfoResolver
        {
            protected static DataTypeName NodeTreeDataTypeName => new("pg_catalog.pg_node_tree");

            TypeInfoMappingCollection? _mappings;
            protected TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new());

            public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
                => Mappings.Find(type, dataTypeName, options);

            static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
            {
                mappings.AddType<string>(NodeTreeDataTypeName,
                    static (options, mapping, _) =>
                    {
                        // Npgsql.Internal.Converters.StringTextConverter is internal. Sigh. Get it using reflection.
                        var stringTextConverterType = Type.GetType("Npgsql.Internal.Converters.StringTextConverter, Npgsql");
                        var stringTextConverter = (PgConverter)Activator.CreateInstance(stringTextConverterType!, options.TextEncoding)!;
                        return mapping.CreateInfo(options, stringTextConverter, preferredFormat: DataFormat.Text);
                    },
                    isDefault: true);
                return mappings;
            }
        }

        class ArrayResolver : Resolver, IPgTypeInfoResolver
        {
            TypeInfoMappingCollection? _mappings;
            new TypeInfoMappingCollection Mappings => _mappings ??= AddMappings(new(base.Mappings));

            public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
                => Mappings.Find(type, dataTypeName, options);

            static TypeInfoMappingCollection AddMappings(TypeInfoMappingCollection mappings)
            {
                mappings.AddArrayType<string>(NodeTreeDataTypeName);
                return mappings;
            }
        }
    }
}
