# OzmaDB Python Client

OzmaDB Python Client is a robust and easy-to-use client library for interacting with the OzmaDB API. It allows you to efficiently manage customizable metadata, execute queries, handle data persistence, and enforce security roles. Ideal for developers building on the ozma.io platform, this client simplifies integration with the powerful open-source OzmaDB engine.

## Table of Contents

- [OzmaDB Python Client](#ozmadb-python-client)
  - [Table of Contents](#table-of-contents)
  - [OzmaDB Features](#ozmadb-features)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Authentication](#authentication)
    - [Instance Management](#instance-management)
    - [Database Operations](#database-operations)
    - [Schemas Management](#schemas-management)
  - [License](#license)
  - [Contact and Support](#contact-and-support)

## OzmaDB Features

- **Customizable Metadata**: All information about schemas, tables, columns, and permissions is stored in tables and can be edited in real-time.
- **System Fields**: Each entity has system fields like `id` and `sub_entity` which are automatically managed.
- **System Tables**: Comprehensive management of schemas, entities, columns, constraints, indexes, and more.
- **Public API**: Interact directly with your data through a public API.
- **Security Roles**: Fine-grained security roles to control access to data.
- **Open-Source**: Completely open-source project.

## Installation

To install the OzmaDB Python Client, run:

```sh
pip install ozmadb-py
```

## Usage
### Authentication
To authenticate with the OzmaDB API, you need to create an instance of `OzmaAuth`:

```python
from ozmadb.auth import OzmaAuth

auth = OzmaAuth(
    client_id='your_client_id',
    client_secret='your_client_secret',
    login='your_login',
    password='your_password'
)
```

### Instance Management
To manage instances, use the `OzmaInstancesAPI`:

```python 
from ozmadb.instances import OzmaInstancesAPI, NewInstance

instances_api = OzmaInstancesAPI(auth)

# Get instances
instances = await instances_api.get_instances()

# Create a new instance
new_instance = NewInstance(name='new_instance_name')
created_instance = await instances_api.create_instance(new_instance)

# Delete an instance
await instances_api.delete_instance('instance_name')
```

### Database Operations
To perform database operations, use the `OzmaDBAPI`:

```python
from ozmadb.ozmadb import OzmaDBAPI

db_api = OzmaDBAPI(auth, name='your_instance_name')

# Get a named user view
result = await db_api.get_named_user_view('schema_name', 'view_name')

# Run a transaction
from ozmadb_py.types import OzmaDBInsertEntityOp, OzmaDBEntityRef

operations = [
    OzmaDBInsertEntityOp(
        entity=OzmaDBEntityRef(schema_name='your_schema', name='your_entity'),
        fields={'field_name': 'value'}
    )
]
transaction_result = await db_api.run_transaction(operations)

# Run an action
action_result = await db_api.run_action('schema_name', 'action_name')
```

### Schemas Management

```python
from ozmadb.ozmadb import OzmaDBAPI

db_api = OzmaDBAPI(auth, name='your_instance_name')

# Save schemas
saved_schemas = await db_api.save_schemas(schemas=['schema1', 'schema2'])

# Restore schemas
await db_api.restore_schemas(saved_schemas, drop_others=True)
```

## License
This project is licensed under the Apache 2.0 License. See the [LICENSE](https://github.com/ozma-io/ozmadb/blob/9d5d920aec8d71ed690404a070d4ed6906e2ca1f/LICENSE) file for more details.

## Contact and Support

For more information, visit [OzmaDB](https://github.com/ozma-io/ozmadb) repository.

If you have any questions or need support, feel free to reach out to our community or contact us via our [Discord](https://discord.gg/Mc8YcF63yt).



