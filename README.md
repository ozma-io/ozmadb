# OzmaDB - REST-accessible SQL-like Database with Embedded JavaScript

OzmaDB is the core database engine for the ozma.io platform. It provides robust features for query execution, data persistence, permission checking, and much more. One of the key principles of OzmaDB is its approach to customization: "all your metadata is also data."

## Features

- **Customizable Metadata**: All information about schemas, tables, columns, and permissions is stored in tables and can be edited in real-time.
- **System Fields**: Each entity has system fields like `id` and `sub_entity` which are automatically managed.
- **System Tables**: Comprehensive management of schemas, entities, columns, constraints, indexes, and more.
- **Public API**: Interact directly with your data through a public API.
- **Security Roles**: Fine-grained security roles to control access to data.
- **Open-Source**: Completely open-source project.

## System Tables

OzmaDB uses several system tables stored in the public schema to describe the current database settings:

- **schemas**: Contains generated schemas.
- **entities**: Contains created entities.
- **column_fields**: Contains columns for tables.
- **computed_fields**: Contains calculated columns.
- **unique_constraints**: Contains unique constraints.
- **check_constraints**: Contains data restrictions.
- **indexes**: Contains indexes for tables.
- **user_views**: Contains named queries that can be called.
- **actions**: Contains server-side functions working within a single transaction.
- **triggers**: Contains a list of triggers executed on specified operations.
- **roles**: Contains a list of roles restricting access to the database.
- **role_entities**: Contains restrictions set for given roles and entities.
- **role_column_fields**: Contains restrictions for given roles and entity fields.
- **users**: Contains a list of users allowed to access the database.
- **events**: Contains the event log in the database.

## Public API

OzmaDB provides a public API through which you can interact directly with your data. This API is used by FunApp and can be leveraged for custom solutions. Note that the API is currently not in a stable version.

## Cloud deployment Options

You can deploy OzmaDB on various cloud providers. Here are some options:

- [Placeholder for Heroku deployment guide]
- [Placeholder for Render deployment guide]
- [Placeholder for Railway deployment guide]

## Installation locally

To install OzmaDB, follow these steps:

1. Clone the repository:

   ```sh
   git clone https://github.com/ozma-io/ozmadb.git
   ```

2. Navigate to the project directory:

   ```sh
   cd ozmadb
   ```

3. Follow the setup instructions in the `dev/setup.sh` script.

## Examples

Here are some basic examples to get you started:

// Placeholder for example of usage

## Contact and Support

For more information, visit [wiki.ozma.io/en/docs/fundb](https://wiki.ozma.io/en/docs/fundb).

If you have any questions or need support, feel free to reach out to our community or contact us via our [Discord](https://discord.gg/Mc8YcF63yt).
