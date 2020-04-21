// FIXME: modernize this as Jint updates or we somehow switch to V8

var commonViews = {
    // Internal APIs
    "settings":
        "SELECT\n" +
        "  name,\n" +
        "  value\n" +
        "FROM\n" +
        "  funapp.settings",
    // Public APIs
    "system_menu":
        "SELECT\n" +
        "  @type = 'menu',\n" +
        "  schemas.name AS category_name,\n" +
        "  entities.name AS name @{ linked_view = { schema: 'funapp', name: 'table-' || schemas.name || '-' || entities.name } }\n" +
        "FROM\n" +
        "  public.entities\n" +
        "  LEFT JOIN public.schemas ON schemas.id = entities.schema_id\n" +
        "WHERE NOT entities.is_hidden\n" +
        "ORDER BY entities.id",
    "user_view_by_name":
        "{ $schema string, $name string }:\n" +
        "SELECT\n" +
        "  @type = 'form',\n" +
        "  @block_sizes = [\n" +
        "    6, 6,\n" +
        "    12\n" +
        "  ],\n" +
        "  schema_id,\n" +
        "  name,\n" +
        "  allow_broken,\n" +
        "  query @{\n" +
	"    form_block = 2,\n" +
	"    text_type = 'codeeditor',\n" +
        "  }\n" +
        "FROM\n" +
        "  public.user_views\n" +
        "WHERE schema_id=>name = $schema AND name = $name\n" +
        "FOR INSERT INTO public.user_views"
};

function addDefaultViews(views, layout) {
    for (var schemaName in layout.schemas) {
        var schema = layout.schemas[schemaName]
        for (var entityName in schema.entities) {
            var entity = schema.entities[entityName];
            if (entity.isHidden) {
                continue;
            }
            var sqlName = renderSqlName(schemaName) + "." + renderSqlName(entityName);
            var fields = [];
            if (entity.children.length > 0) {
                fields.push(renderSqlName("sub_entity"))
            }
            for (var columnField in entity.columnFields) {
                fields.push(renderSqlName(columnField));
            }
            for (var computedField in entity.computedFields) {
                var comp = entity.computedFields[computedField];
                if (!comp.broken) {
                    fields.push(renderSqlName(computedField));
                }
            }

            var formName = "form-" + schemaName + "-" + entityName;
            var formQuery =
                "{ $id reference(" + sqlName + ") }:\n\n" +
                "SELECT\n  " +
                ["@type = 'form'"].concat(fields).join(",\n  ") +
                "\nFROM " + sqlName + " " +
                "WHERE id = $id";
            if (!entity.isAbstract) {
                formQuery = formQuery + "\nFOR INSERT INTO " + sqlName;
            }
            views[formName] = formQuery;

            var tableName = "table-" + schemaName + "-" + entityName;
            var tableQuery =
                "SELECT\n  " +
                [ "@create_view = &\"" + formName + "\"",
                  "id @{ row_linked_view = &\"" + formName + "\" }"
                ].concat(fields).join(",\n  ") +
                "\nFROM " + sqlName +
                "\nORDER BY id";
            if (!entity.isAbstract) {
                formQuery = formQuery + "\nFOR INSERT INTO " + sqlName;
            }
            views[tableName] = tableQuery;
        }
    }
}

function GetUserViews(layout) {
    var newViews = JSON.parse(JSON.stringify(commonViews));
    addDefaultViews(newViews, layout);
    return newViews;
}
