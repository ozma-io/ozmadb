# OzmaDB API

## Overview

OzmaDB API is a powerful, open-source API client for interacting with OzmaDB - the ultimate open-source database solution for customizable metadata. This API client provides robust methods for query execution, data persistence, permission checking, and much more.

## Features

- **Easy Integration**: Quickly integrate with OzmaDB to manage your data.
- **Rich Functionality**: Supports a wide range of operations including querying, inserting, updating, and deleting entities.
- **Customizable Metadata**: Interact with customizable metadata for schemas, tables, columns, and permissions.
- **Security Roles**: Utilize fine-grained security roles to control access to your data.
- **Open-Source**: Completely open-source and community-driven.

## Installation

Install the OzmaDB API client using npm:

```sh
npm install @ozma-io/ozmadb-js
```

## Usage

### Basic Example
```js
import OzmaDBClient from "@ozma-io/ozmadb-js/client";

async function exampleUsage() {
  try {
    const userView = await OzmaDBClient.getNamedUserView({ schema: 'user', name: 'example_view' });
    console.log(userView);
  } catch (error) {
    if (error instanceof OzmaDBError) {
      console.error('API Error:', error.body);
    } else {
      console.error('Unexpected Error:', error);
    }
  }
}

exampleUsage();
```

### API Methods
`getNamedUserView(ref: IUserViewRef, args?: Record<string, unknown>, opts?: IEntriesRequestOpts): Promise<IViewExprResult>`

Retrieve a named user view with optional arguments and options.

Example:

```js
const ref = { schema: 'user', name: 'example_view' };
const args = { archived: false };
const opts = { chunk: { limit: 10, offset: 0 } };

OzmaDBClient.getNamedUserView(ref, args, opts)
  .then(view => console.log(view))
  .catch(error => console.error('Error fetching user view:', error));
```

`insertEntity(ref: IEntityRef, fields: Record<FieldName, unknown>): Promise<RowId | null>`

Insert a new entity into the database.

Example:

```js
const entityRef = { schema: 'user', name: 'customers' };
const newCustomer = {
  name: 'John Doe',
  email: 'john.doe@example.com',
  active: true
};

OzmaDBClient.insertEntity(entityRef, newCustomer)
  .then(entityId => console.log('Inserted customer ID:', entityId))
  .catch(error => console.error('Error inserting customer:', error));
```

`updateEntity(ref: IEntityRef, id: RowKey, fields: Record<FieldName, unknown>): Promise<RowId>`

Update an existing entity in the database.

Example:

```js
const entityRef = { schema: 'user', name: 'customers' };
const customerId = 1; // assuming customer ID is 1
const updatedData = {
  email: 'john.new@example.com'
};

OzmaDBClient.updateEntity(entityRef, customerId, updatedData)
  .then(entityId => console.log('Updated customer ID:', entityId))
  .catch(error => console.error('Error updating customer:', error));
```

`deleteEntity(ref: IEntityRef, id: RowKey): Promise<void>`

Delete an entity from the database.

Example:
```js
const entityRef = { schema: 'user', name: 'customers' };
const customerId = 1; // assuming customer ID is 1

OzmaDBClient.deleteEntity(entityRef, customerId)
  .then(() => console.log('Customer deleted successfully.'))
  .catch(error => console.error('Error deleting customer:', error));
```

### All API Methods

| Method | Input Parameters | Returning Value | Description |
|--------|------------------|----------------------|-------------|
| `insertEntity` | `ref: IEntityRef, fields: Record<FieldName, unknown>` | `Promise<RowId> \| <null>` | Insert a new entity into the database. |
| `updateEntity` | `ref: IEntityRef, id: RowKey, fields: Record<FieldName, unknown>` | `Promise<RowId>` | Update an existing entity in the database. |
| `deleteEntity` | `ref: IEntityRef, id: RowKey` | `Promise<void>` | Delete an entity from the database. |
| `getNamedUserView` | `ref: IUserViewRef, args?: Record<string, unknown>, opts?: IEntriesRequestOpts` | `Promise<IViewExprResult>` | Retrieve a named user view with optional arguments and options. |
| `getAnonymousUserView` | `query: string, args?: Record<string, unknown>, opts?: IEntriesRequestOpts` | `Promise<IViewExprResult>` | Retrieve an anonymous user view with optional arguments and options. |
| `getNamedUserViewInfo` | `ref: IUserViewRef, opts?: IInfoRequestOpts` | `Promise<IViewInfoResult>` | Retrieve information about a named user view. |
| `getAnonymousUserViewExplain` | `query: string, args?: Record<string, unknown>, opts?: IEntriesExplainOpts` | `Promise<IViewExplainResult>` | Explain an anonymous user view query. |
| `getNamedUserViewExplain` | `ref: IUserViewRef, args?: Record<string, unknown>, opts?: IEntriesExplainOpts` | `Promise<IViewExplainResult>` | Explain a named user view query. |
| `getEntityInfo` | `ref: IEntityRef` | `Promise<IEntity>` | Retrieve information about an entity. |
| `runTransaction` | `action: ITransaction` | `Promise<ITransactionResult>` | Run a transaction with multiple operations. |
| `runAction` | `ref: IActionRef, args?: Record<string, unknown>` | `Promise<IActionResult>` | Run a server-side action. |
| `getDomainValues` | `ref: IFieldRef, rowId?: number, opts?: IEntriesRequestOpts` | `Promise<IDomainValuesResult>` | Retrieve domain values for a field. |
| `getDomainExplain` | `ref: IFieldRef, rowId?: number, opts?: IEntriesExplainOpts` | `Promise<IExplainedQuery>` | Explain a domain query. |
| `saveSchemas` | `schemas: string[] \| "all", options?: ISaveSchemasOptions` | `Promise<Blob>` | Save schemas to a file. |
| `restoreSchemas` | `data: Blob, options?: IRestoreSchemasOptions` | `Promise<void>` | Restore schemas from a file. |
| `getPermissions` |  | `Promise<IPermissionsInfo>` | Retrieve permission information. |


## Examples
Here are some detailed examples to get you started:

### Inserting an Entity

```js
async function insertExample() {
  const entityRef = { schema: 'user', name: 'products' };
  const newProduct = {
    name: 'Coffee Beans',
    price: 12.99,
    stock: 100
  };

  try {
    const entityId = await OzmaDBClient.insertEntity(entityRef, newProduct);
    console.log('Inserted product ID:', entityId);
  } catch (error) {
    console.error('Error inserting product:', error);
  }
}

insertExample();
```

### Updating an Entity

```js 
async function updateExample() {
  const entityRef = { schema: 'user', name: 'products' };
  const productId = 1; // assuming product ID is 1
  const updatedData = {
    price: 10.99
  };

  try {
    const entityId = await OzmaDBClient.updateEntity(entityRef, productId, updatedData);
    console.log('Updated product ID:', entityId);
  } catch (error) {
    console.error('Error updating product:', error);
  }
}

updateExample();
```

### Deleting an Entity

```js
async function deleteExample() {
  const entityRef = { schema: 'user', name: 'products' };
  const productId = 1; // assuming product ID is 1

  try {
    await OzmaDBClient.deleteEntity(entityRef, productId);
    console.log('Product deleted successfully.');
  } catch (error) {
    console.error('Error deleting product:', error);
  }
}

deleteExample();
```

### Running a Transaction

```js
async function runTransactionExample() {
  const insertOp = {
    type: "insert",
    entity: { schema: 'public', name: 'orders' },
    fields: {
      customerId: 1,
      orderDate: '2023-07-01',
      total: 99.99
    }
  };

  const updateOp = {
    type: "update",
    entity: { schema: 'public', name: 'customers' },
    id: 1,
    fields: {
      active: true
    }
  };

  const transaction = {
    operations: [insertOp, updateOp],
    deferConstraints: true
  };

  try {
    const result = await OzmaDBClient.runTransaction(transaction);
    console.log('Transaction result:', result);
  } catch (error) {
    console.error('Error running transaction:', error);
  }
}

runTransactionExample();
```

### Fetching Domain Values

```js
async function getDomainValuesExample() {
  const fieldRef = {
    entity: { schema: 'user', name: 'products' },
    name: 'category'
  };

  try {
    const domainValues = await OzmaDBClient.getDomainValues(fieldRef);
    console.log('Domain values:', domainValues);
  } catch (error) {
    console.error('Error fetching domain values:', error);
  }
}

getDomainValuesExample();
```

### Explaining a Query

```js
async function explainQueryExample() {
  const query = 'SELECT * FROM user.products WHERE price > 10';
  const args = {};
  const opts = { verbose: true };

  try {
    const explanation = await OzmaDBClient.getAnonymousUserViewExplain(query, args, opts);
    console.log('Query explanation:', explanation);
  } catch (error) {
    console.error('Error explaining query:', error);
  }
}

explainQueryExample();
```

## License
This project is licensed under the Apache 2.0 License. See the [LICENSE](https://github.com/ozma-io/ozmadb/blob/9d5d920aec8d71ed690404a070d4ed6906e2ca1f/LICENSE) file for more details.

## Contact and Support

For more information, visit [OzmaDB](https://github.com/ozma-io/ozmadb) repository.

If you have any questions or need support, feel free to reach out to our community or contact us via our [Discord](https://discord.gg/Mc8YcF63yt).


