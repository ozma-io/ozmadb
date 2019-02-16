// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "Settings": "select\n  \"Name\",\n  \"Value\"\nfrom \n  \"funapp\".\"Settings\"",
    "FieldTranslations": "select\n  \"Schemas\".\"Name\" as \"SchemaName\",\n  \"Entities\".\"Name\" as \"EntityName\",\n  \"ColumnFields\".\"Name\" as \"FieldName\",\n  \"ColumnFieldTranslations\".\"Translation\"\nfrom \n  \"funapp\".\"ColumnFieldTranslations\"\n  left join \"public\".\"ColumnFields\" on \"ColumnFields\".\"Id\" = \"ColumnFieldTranslations\".\"ColumnFieldId\"\n  left join \"public\".\"Entities\" on \"Entities\".\"Id\" = \"ColumnFields\".\"EntityId\"\n  left join \"public\".\"Schemas\" on \"Schemas\".\"Id\" = \"Entities\".\"SchemaId\"\n  where $$Lang LIKE \"ColumnFieldTranslations\".\"Language\" || '%'",
    // Public APIs
    "TranslatedMenu": "select\n    @\"Type\" = 'Menu',\n    \"MainMenuCategories\".\"Name\" as \"CategoryName\",\n    { \"LinkedView\" = \"UserViews\".\"Name\" } COALESCE(\"Translations\".\"Translation\", \"UserViews\".\"Name\") as \"Name\"\n    from\n    \"funapp\".\"MainMenuButtons\"\n    left join \"funapp\".\"MainMenuCategories\" on \"MainMenuCategories\".\"Id\" = \"MainMenuButtons\".\"CategoryId\"\n    left join \"public\".\"UserViews\" on \"UserViews\".\"Id\" = \"MainMenuButtons\".\"UserViewId\"\n    left join (select \"UserViewId\", \"Translation\" from \"funapp\".\"UserViewTranslations\" where $$Lang LIKE \"Language\" || '%') as \"Translations\" on \"Translations\".\"UserViewId\" = \"MainMenuButtons\".\"UserViewId\"\n    order by \"MainMenuCategories\".\"OrdinalPosition\", \"MainMenuButtons\".\"OrdinalPosition\""
}

function GetSystemUserViews(layout) {
    var newViews = JSON.parse(JSON.stringify(commonViews))

    // Add summary views for each entity
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName]
            var name = "Summary__" + schemaName + "__" + entityName
            var query = "select \"Id\", __main as \"Main\" from " + renderSqlName(schemaName) + "." + renderSqlName(entityName)
            newViews[name] = query
        }
    }

    return newViews
}