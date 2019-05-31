// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "Settings":
        "SELECT\n" +
        "  \"Name\",\n" +
        "  \"Value\"\n" +
        "FROM\n" +
        "  \"funapp\".\"Settings\"",
    "FieldTranslations":
        "SELECT\n" +
        "  \"SchemaName\",\n" +
        "  \"EntityName\",\n" +
        "  \"FieldName\",\n" +
        "  \"Translation\"\n" +
        "FROM\n" +
        "  \"funapp\".\"FieldTranslations\"\n" +
        "WHERE $$Lang LIKE \"FieldTranslations\".\"Language\" || '%'",
    // Public APIs
    "TranslatedMenu":
        "SELECT\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"MainMenuCategories\".\"Name\" AS \"CategoryName\",\n" +
        "  COALESCE(\"Translations\".\"Translation\", \"UserViews\".\"Name\") AS \"Name\" @{ \"LinkedView\" = { schema: \"UserViews\".\"SchemaName\", name: \"UserViews\".\"Name\" } }\n" +
        "FROM\n" +
        "  \"funapp\".\"MainMenuButtons\"\n" +
        "  LEFT JOIN \"funapp\".\"MainMenuCategories\" ON \"MainMenuCategories\".\"Id\" = \"MainMenuButtons\".\"CategoryId\"\n" +
        "  INNER JOIN (SELECT \"UserViews\".\"Name\", \"UserViews\".\"SchemaId\"=>\"Name\" AS \"SchemaName\" FROM \"public\".\"UserViews\") AS \"UserViews\" ON \"UserViews\".\"SchemaName\" = \"MainMenuButtons\".\"SchemaName\" AND \"UserViews\".\"Name\" = \"MainMenuButtons\".\"UserViewName\"\n" +
        "  LEFT JOIN (SELECT \"SchemaName\", \"UserViewName\", \"Translation\" FROM \"funapp\".\"UserViewTranslations\" where $$Lang LIKE \"Language\" || '%') as \"Translations\"\n" +
        "    ON \"Translations\".\"SchemaName\" = \"UserViews\".\"SchemaName\" AND \"Translations\".\"UserViewName\" = \"UserViews\".\"Name\"\n" +
        "ORDER BY \"MainMenuCategories\".\"OrdinalPosition\", \"MainMenuButtons\".\"OrdinalPosition\"",
    "SystemMenu":
        "SELECT\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"Schemas\".\"Name\" AS \"CategoryName\",\n" +
        "  \"Entities\".\"Name\" AS \"Name\" @{ \"LinkedView\" = { schema: 'funapp', name: 'Table-' || \"Schemas\".\"Name\" || '-' || \"Entities\".\"Name\" } }\n" +
        "FROM\n" +
        "  \"public\".\"Entities\"\n" +
        "  LEFT JOIN \"public\".\"Schemas\" ON \"Schemas\".\"Id\" = \"Entities\".\"SchemaId\"\n" +
        "ORDER BY \"Entities\".\"Id\"",
    "UserViewByName":
        "( $schema string, $name string ):\n" +
        "SELECT\n" +
        "  @\"Type\" = 'Form',\n" +
        "  \"SchemaId\",\n" +
        "  \"Name\",\n" +
        "  \"Query\" @{ \"TextType\" = 'codeeditor' }\n" +
        "FROM\n" +
        "  \"public\".\"UserViews\"\n" +
        "WHERE \"SchemaId\"=>\"Name\" = $schema AND \"Name\" = $name\n" +
        "FOR INSERT INTO \"public\".\"UserViews\""
}

function addSummaryViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName]
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName)
            var name = "Summary-" + schemaName + "-" + entityName
            var query = "SELECT \"Id\", __main AS \"Main\" FROM " + sqlName + " ORDER BY __main"
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
                "SELECT\n  " +
                ["@\"Type\" = 'Form'"].concat(fields).join(",") +
                "\nFROM " + sqlName + " " +
                "WHERE \"Id\" = $id" +
                "\nFOR INSERT INTO " + sqlName
            views[formName] = formQuery

            var tableName = "Table-" + schemaName + "-" + entityName
            var tableQuery =
                "SELECT\n  " +
                [ "@\"CreateView\" = &\"" + formName + "\"",
                  "\"Id\" @{ \"RowLinkedView\" = &\"" + formName + "\" }"
                ].concat(fields).join(", ") +
                "\nFROM " + sqlName +
                "\nORDER BY \"Id\"\n" +
                "FOR INSERT INTO " + sqlName
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
