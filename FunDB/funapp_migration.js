// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "Settings":
        "select\n" +
        "  \"Name\",\n" +
        "  \"Value\"\n" +
        "from\n" +
        "  \"funapp\".\"Settings\"",
    "FieldTranslations":
        "select\n" +
        "  \"SchemaName\",\n" +
        "  \"EntityName\",\n" +
        "  \"FieldName\",\n" +
        "  \"Translation\"\n" +
        "from\n" +
        "  \"funapp\".\"FieldTranslations\"\n" +
        "where $$Lang LIKE \"FieldTranslations\".\"Language\" || '%'",
    // Public APIs
    "TranslatedMenu":
        "select\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"MainMenuCategories\".\"Name\" as \"CategoryName\",\n" +
        "  COALESCE(\"Translations\".\"Translation\", \"UserViews\".\"Name\") as \"Name\" @{ \"LinkedView\" = { schema: \"UserViews\".\"SchemaName\", name: \"UserViews\".\"Name\" } }\n" +
        "from\n" +
        "  \"funapp\".\"MainMenuButtons\"\n" +
        "  left join \"funapp\".\"MainMenuCategories\" on \"MainMenuCategories\".\"Id\" = \"MainMenuButtons\".\"CategoryId\"\n" +
        "  inner join (select \"UserViews\".\"Name\", \"UserViews\".\"SchemaId\"=>\"Name\" as \"SchemaName\" from \"public\".\"UserViews\") as \"UserViews\" on \"UserViews\".\"SchemaName\" = \"MainMenuButtons\".\"SchemaName\" and \"UserViews\".\"Name\" = \"MainMenuButtons\".\"UserViewName\"\n" +
        "  left join (select \"SchemaName\", \"UserViewName\", \"Translation\" from \"funapp\".\"UserViewTranslations\" where $$Lang LIKE \"Language\" || '%') as \"Translations\"\n" +
        "    on \"Translations\".\"SchemaName\" = \"UserViews\".\"SchemaName\" and \"Translations\".\"UserViewName\" = \"UserViews\".\"Name\"\n" +
        "order by \"MainMenuCategories\".\"OrdinalPosition\", \"MainMenuButtons\".\"OrdinalPosition\"",
    "SystemMenu":
        "select\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"Schemas\".\"Name\" as \"CategoryName\",\n" +
        "  \"Entities\".\"Name\" as \"Name\" @{ \"LinkedView\" = { schema: 'funapp', name: 'Table-' || \"Schemas\".\"Name\" || '-' || \"Entities\".\"Name\" } }\n" +
        "from\n" +
        "  \"public\".\"Entities\"\n" +
        "  left join \"public\".\"Schemas\" on \"Schemas\".\"Id\" = \"Entities\".\"SchemaId\"\n" +
        "order by \"Entities\".\"Id\"",
    "UserViewByName":
        "( $schema string, $name string ):\n" +
        "select\n" +
        "  @\"Type\" = 'Form',\n" +
        "  \"SchemaId\",\n" +
        "  \"Name\",\n" +
        "  \"Query\" @{ \"TextType\" = 'codeeditor' }\n" +
        "from\n" +
        "  \"public\".\"UserViews\"\n" +
        "where \"SchemaId\"=>\"Name\" = $schema and \"Name\" = $name\n" +
        "for insert into \"public\".\"UserViews\""
}

function addSummaryViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName]
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName)
            var name = "Summary-" + schemaName + "-" + entityName
            var query = "select \"Id\", __main as \"Main\" from " + sqlName + " order by __main"
            views[name] = query
        }
    }
}

function addDefaultViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName]
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName)
            var fields = []
            for (var columnField in entity.columnFields) {
                fields.push(renderSqlName(columnField))
            }
            for (var computedField in entity.computedFields) {
                fields.push(renderSqlName(computedField))
            }

            var formName = "Form-" + schemaName + "-" + entityName
            var formQuery =
                "( $id reference(" + sqlName + ") ):\n" +
                "select\n  " +
                ["@\"Type\" = 'Form'"].concat(fields).join(",") +
                "\nfrom " + sqlName + " " +
                "where \"Id\" = $id for insert into " + sqlName
            views[formName] = formQuery

            var tableName = "Table-" + schemaName + "-" + entityName
            var tableQuery =
                "select\n  " +
                [ "@\"CreateView\" = &\"" + formName + "\"",
                  "\"Id\" @{ \"RowLinkedView\" = &\"" + formName + "\" }"
                ].concat(fields).join(", ") +
                "\nfrom " + sqlName + " " +
                "for insert into " + sqlName
            views[tableName] = tableQuery
        }
    }
}

function GetUserViews(layout) {
    var newViews = JSON.parse(JSON.stringify(commonViews))
    addSummaryViews(newViews, layout)
    addDefaultViews(newViews, layout)
    return newViews
}
