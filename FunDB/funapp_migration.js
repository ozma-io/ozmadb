// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "Settings":
        "SELECT\n" +
        "  \"Name\",\n" +
        "  \"Value\"\n" +
        "FROM\n" +
        "  \"funapp\".\"Settings\"",
    // Public APIs
    "SystemMenu":
        "SELECT\n" +
        "  @\"Type\" = 'Menu',\n" +
        "  \"Schemas\".\"Name\" AS \"CategoryName\",\n" +
        "  \"Entities\".\"Name\" AS \"Name\" @{ \"LinkedView\" = { schema: 'funapp', name: 'Table-' || \"Schemas\".\"Name\" || '-' || \"Entities\".\"Name\" } }\n" +
        "FROM\n" +
        "  \"public\".\"Entities\"\n" +
        "  LEFT JOIN \"public\".\"Schemas\" ON \"Schemas\".\"Id\" = \"Entities\".\"SchemaId\"\n" +
        "WHERE NOT \"Entities\".\"Hidden\"\n" +
        "ORDER BY \"Entities\".\"Id\"",
    "UserViewByName":
        "{ $schema string, $name string }:\n" +
        "SELECT\n" +
        "  @\"Type\" = 'Form',\n" +
        "  \"SchemaId\",\n" +
        "  \"Name\",\n" +
        "  \"AllowBroken\",\n" +
        "  \"Query\" @{ \"TextType\" = 'codeeditor' }\n" +
        "FROM\n" +
        "  \"public\".\"UserViews\"\n" +
        "WHERE \"SchemaId\"=>\"Name\" = $schema AND \"Name\" = $name\n" +
        "FOR INSERT INTO \"public\".\"UserViews\""
};

function addDefaultViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName];
            if (entity.hidden) {
                continue;
            }
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName);
            var fields = [];
            if (entity.children.length > 0) {
                fields.push(renderSqlName("SubEntity"))
            }
            for (var columnField in entity.columnFields) {
                fields.push(renderSqlName(columnField));
            }
            for (var computedField in entity.computedFields) {
                fields.push(renderSqlName(computedField));
            }

            var formName = "Form-" + schemaName + "-" + entityName;
            var formQuery =
                "{ $id reference(" + sqlName + ") }:\n\n" +
                "SELECT\n  " +
                ["@\"Type\" = 'Form'"].concat(fields).join(",\n  ") +
                "\nFROM " + sqlName + " " +
                "WHERE \"Id\" = $id" +
                "\nFOR INSERT INTO " + sqlName;
            views[formName] = formQuery;

            var tableName = "Table-" + schemaName + "-" + entityName;
            var tableQuery =
                "SELECT\n  " +
                [ "@\"CreateView\" = &\"" + formName + "\"",
                  "\"Id\" @{ \"RowLinkedView\" = &\"" + formName + "\" }"
                ].concat(fields).join(",\n  ") +
                "\nFROM " + sqlName +
                "\nORDER BY \"Id\"\n" +
                "FOR INSERT INTO " + sqlName;
            views[tableName] = tableQuery;
        }
    }
}

function GetUserViews(layout) {
    var newViews = JSON.parse(JSON.stringify(commonViews));
    addDefaultViews(newViews, layout);
    return newViews;
}
