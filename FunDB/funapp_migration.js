// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "__Settings":
        "select\n" +
        "  \"Name\",\n" +
        "  \"Value\"\n" +
        "from\n" +
        "  \"funapp\".\"Settings\"",
    "__FieldTranslations":
        "select\n" +
        "  \"Schemas\".\"Name\" as \"SchemaName\",\n" +
        "  \"Entities\".\"Name\" as \"EntityName\",\n" +
        "  \"ColumnFields\".\"Name\" as \"FieldName\",\n" +
        "  \"ColumnFieldTranslations\".\"Translation\"\n" +
        "from\n" +
        "  \"funapp\".\"ColumnFieldTranslations\"" +
        "  left join \"public\".\"ColumnFields\" on \"ColumnFields\".\"Id\" = \"ColumnFieldTranslations\".\"ColumnFieldId\"\n" +
        "  left join \"public\".\"Entities\" on \"Entities\".\"Id\" = \"ColumnFields\".\"EntityId\"\n" +
        "  left join \"public\".\"Schemas\" on \"Schemas\".\"Id\" = \"Entities\".\"SchemaId\"\n" +
        "where $$Lang LIKE \"ColumnFieldTranslations\".\"Language\" || '%'",
    // Public APIs
    "__TranslatedMenu":
        "select\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"MainMenuCategories\".\"Name\" as \"CategoryName\",\n" +
        "  { \"LinkedView\" = \"UserViews\".\"Name\" } COALESCE(\"Translations\".\"Translation\", \"UserViews\".\"Name\") as \"Name\"\n" +
        "from\n" +
        "  \"funapp\".\"MainMenuButtons\"\n" +
        "  left join \"funapp\".\"MainMenuCategories\" on \"MainMenuCategories\".\"Id\" = \"MainMenuButtons\".\"CategoryId\"\n" +
        "  left join \"public\".\"UserViews\" on \"UserViews\".\"Id\" = \"MainMenuButtons\".\"UserViewId\"\n" +
        "  left join (select \"UserViewId\", \"Translation\" from \"funapp\".\"UserViewTranslations\" where $$Lang LIKE \"Language\" || '%') as \"Translations\" on \"Translations\".\"UserViewId\" = \"MainMenuButtons\".\"UserViewId\"\n" +
        "order by \"MainMenuCategories\".\"OrdinalPosition\", \"MainMenuButtons\".\"OrdinalPosition\""/*,
    "__SystemMenu":
        "select\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  'All' as \"CategoryName\",\n" +
        "  { \"LinkedView\" = \"UserViews\".\"Name\" } COALESCE(\"Translations\".\"Translation\", \"UserViews\".\"Name\") as \"Name\"\n" +
        "from\n" +
        "  \"public\".\"UserViews\"\n" +
        "  left join (select \"UserViewId\", \"Translation\" from \"funapp\".\"UserViewTranslations\" where $$Lang LIKE \"Language\" || '%') as \"Translations\" on \"Translations\".\"UserViewId\" = \"MainMenuButtons\".\"UserViewId\"\n" +
        "where \"UserViews\".\"Name\" LIKE '__Table__%'\n" +
        "order by \"MainMenuCategories\".\"OrdinalPosition\", \"MainMenuButtons\".\"OrdinalPosition\""*/
}

function addSummaryViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName]
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName)
            var name = "__Summary__" + schemaName + "__" + entityName
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

            var formName = "__Form__" + schemaName + "__" + entityName
            var formQuery =
                "( $id reference(" + sqlName + ") ) =>\n" +
                "select " +
                ["@\"Type\" = 'Form'"].concat(fields).join(", ") +
                "from " + sqlName + " " +
                "where \"Id\" = $id for update of " + sqlName
            views[formName] = formQuery

            var tableName = "__Table__" + schemaName + "__" + entityName
            var tableQuery =
                "select " +
                [ "@\"CreateView\" = '" + formName + "'",
                  "@\"LinkedView\" = '" + formName + "'",
                  "\"Id\""
                ].concat(fields).join(", ") +
                "from " + sqlName + " " +
                "for update of " + sqlName
            views[tableName] = tableQuery
        }
    }
}

function GetSystemUserViews(layout) {
    var newViews = JSON.parse(JSON.stringify(commonViews))
    addSummaryViews(newViews, layout)
    addDefaultViews(newViews, layout)
    return newViews
}