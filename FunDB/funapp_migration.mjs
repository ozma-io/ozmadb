const commonViews = {
    // Internal APIs
    "settings": `
SELECT
  name,
  value
FROM
  funapp.settings`,
    // Public APIs
    "system_menu": `
SELECT
  @type = 'menu',
  schemas.name AS category_name,
  entities.name AS name @{ linked_view = { schema: 'funapp', name: 'table-' || schemas.name || '-' || entities.name } }
FROM
  public.entities
  LEFT JOIN public.schemas ON schemas.id = entities.schema_id
ORDER BY entities.id`,
    "user_view_by_name": `
{ $schema string, $name string }:
SELECT
  @type = 'form',
  @block_sizes = [
    6, 6,
    12
  ],
  schema_id,
  name,
  allow_broken,
  query @{
    form_block = 2,
    text_type = 'codeeditor',
  }
FROM
  public.user_views
WHERE schema_id=>name = $schema AND name = $name
FOR INSERT INTO public.user_views`
};

const generateDefaultViews = (layout) => {
    return Object.fromEntries(Object.entries(layout.schemas).flatMap(([schemaName, schema]) => {
        return Object.entries(schema.entities).flatMap(([entityName, entity]) => {
            if (entity.isHidden) {
                return [];
            }
            const sqlName = `${renderSqlName(schemaName)}.${renderSqlName(entityName)}`;
            const fields = [];
            if (entity.children.length > 0) {
                fields.push(renderSqlName("sub_entity"));
            }
            Object.keys(entity.columnFields).forEach(columnField => {
                fields.push(renderSqlName(columnField));
            });
            Object.entries(entity.computedFields).forEach(([computedField, comp]) => {
                if (!comp.isBroken) {
                    fields.push(renderSqlName(computedField));
                }
            });

            const formName = `form-${schemaName}-${entityName}`;
            const formQuery = `
{ $id reference(${sqlName}) }:
SELECT
  @type = 'form',
  ${fields.join(",\n  ")}
FROM ${sqlName}
WHERE id = $id
${entity.isAbstract ? "" : `FOR INSERT INTO ${sqlName}`}`;

            const tableName = `table-${schemaName}-${entityName}`;
            const tableQuery = `
SELECT
  @type = 'table',
  @create_view = &"${formName}",
  id @{ row_linked_view = &"${formName}" },
  ${fields.join(",\n  ")}
FROM ${sqlName}
ORDER BY id
${entity.isAbstract ? "" : `FOR INSERT INTO ${sqlName}`}`;

            return [[formName, formQuery], [tableName, tableQuery]];
        });
    }));
};

export default function getUserViews(layout) {
    const generated = generateDefaultViews(layout);
    const allViews = { ...commonViews, ...generated };
    return Object.fromEntries(Object.entries(allViews).map(([name, query]) => [name, query.trim()]));
};
