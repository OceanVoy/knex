// PostgreSQL
// -------
const extend = require('lodash/extend');
const map = require('lodash/map');
const { promisify } = require('util');
const Client = require('../../client');

const Transaction = require('./execution/pg-transaction');
const ColumnCompiler = require('./schema/pg-columncompiler');
const TableCompiler = require('./schema/pg-tablecompiler');
const SchemaCompiler = require('./schema/pg-compiler');
const { makeEscape } = require('../../util/string');
const { isString } = require('../../util/is');

const identity = require('lodash/identity');
const reduce = require('lodash/reduce');

const QueryCompiler = require('../../query/querycompiler');
const { wrapString } = require('../../formatter/wrappingFormatter');

class QueryCompilerPG extends QueryCompiler {
  constructor(client, builder, formatter) {
    super(client, builder, formatter);
    this._defaultInsertValue = 'default';
  }

  // Compiles a truncate query.
  truncate() {
    return `truncate ${this.tableName} restart identity`;
  }

  // is used if the an array with multiple empty values supplied

  // Compiles an `insert` query, allowing for multiple
  // inserts using a single query statement.
  insert() {
    let sql = super.insert();
    if (sql === '') return sql;

    const { returning, onConflict, ignore, merge, insert } = this.single;
    if (onConflict && ignore) sql += this._ignore(onConflict);
    if (onConflict && merge) {
      sql += this._merge(merge.updates, onConflict, insert);
      const wheres = this.where();
      if (wheres) sql += ` ${wheres}`;
    }
    if (returning) sql += this._returning(returning);

    return {
      sql: sql,
      returning,
    };
  }

  // Compiles an `update` query, allowing for a return value.
  update() {
    const withSQL = this.with();
    const updateData = this._prepUpdate(this.single.update);
    const wheres = this.where();
    const { returning } = this.single;
    return {
      sql:
        withSQL +
        `update ${this.single.only ? 'only ' : ''}${this.tableName} ` +
        `set ${updateData.join(', ')}` +
        (wheres ? ` ${wheres}` : '') +
        this._returning(returning),
      returning,
    };
  }

  // Compiles an `update` query, allowing for a return value.
  del() {
    const sql = super.del(...arguments);
    const { returning } = this.single;
    return {
      sql: sql + this._returning(returning),
      returning,
    };
  }

  aggregate(stmt) {
    return this._aggregate(stmt, { distinctParentheses: true });
  }

  _returning(value) {
    return value ? ` returning ${this.formatter.columnize(value)}` : '';
  }

  _ignore(columns) {
    return ` on conflict (${this.formatter.columnize(columns)}) do nothing`;
  }

  _merge(updates, columns, insert) {
    let sql = ` on conflict (${this.formatter.columnize(
      columns
    )}) do update set `;
    if (updates && Array.isArray(updates)) {
      sql += updates
        .map((column) =>
          wrapString(
            column.split('.').pop(),
            this.formatter.builder,
            this.client,
            this.formatter
          )
        )
        .map((column) => `${column} = excluded.${column}`)
        .join(', ');

      return sql;
    } else if (updates && typeof updates === 'object') {
      const updateData = this._prepUpdate(updates);
      if (typeof updateData === 'string') {
        sql += updateData;
      } else {
        sql += updateData.join(',');
      }

      return sql;
    } else {
      const insertData = this._prepInsert(insert);
      if (typeof insertData === 'string') {
        throw new Error(
          'If using merge with a raw insert query, then updates must be provided'
        );
      }

      sql += insertData.columns
        .map((column) =>
          wrapString(column.split('.').pop(), this.builder, this.client)
        )
        .map((column) => `${column} = excluded.${column}`)
        .join(', ');

      return sql;
    }
  }

  // Join array of table names and apply default schema.
  _tableNames(tables) {
    const schemaName = this.single.schema;
    const sql = [];

    for (let i = 0; i < tables.length; i++) {
      let tableName = tables[i];

      if (tableName) {
        if (schemaName) {
          tableName = `${schemaName}.${tableName}`;
        }
        sql.push(this.formatter.wrap(tableName));
      }
    }

    return sql.join(', ');
  }

  forUpdate() {
    const tables = this.single.lockTables || [];

    return (
      'for update' + (tables.length ? ' of ' + this._tableNames(tables) : '')
    );
  }

  forShare() {
    const tables = this.single.lockTables || [];

    return (
      'for share' + (tables.length ? ' of ' + this._tableNames(tables) : '')
    );
  }

  skipLocked() {
    return 'skip locked';
  }

  noWait() {
    return 'nowait';
  }

  // Compiles a columnInfo query
  columnInfo() {
    const column = this.single.columnInfo;
    let schema = this.single.schema;

    // The user may have specified a custom wrapIdentifier function in the config. We
    // need to run the identifiers through that function, but not format them as
    // identifiers otherwise.
    const table = this.client.customWrapIdentifier(this.single.table, identity);

    if (schema) {
      schema = this.client.customWrapIdentifier(schema, identity);
    }

    let sql =
      'select * from information_schema.columns where table_name = ? and table_catalog = ?';
    const bindings = [table, this.client.database()];

    if (schema) {
      sql += ' and table_schema = ?';
      bindings.push(schema);
    } else {
      sql += ' and table_schema = current_schema()';
    }

    return {
      sql,
      bindings,
      output(resp) {
        const out = reduce(
          resp.rows,
          function (columns, val) {
            columns[val.column_name] = {
              type: val.data_type,
              maxLength: val.character_maximum_length,
              nullable: val.is_nullable === 'YES',
              defaultValue: val.column_default,
            };
            return columns;
          },
          {}
        );
        return (column && out[column]) || out;
      },
    };
  }

  distinctOn(value) {
    return 'distinct on (' + this.formatter.columnize(value) + ') ';
  }
}

class Client_PG extends Client {
  constructor(config) {
    super(config);
    if (config.returning) {
      this.defaultReturning = config.returning;
    }

    if (config.searchPath) {
      this.searchPath = config.searchPath;
    }
  }
  transaction() {
    return new Transaction(this, ...arguments);
  }

  queryCompiler(builder, formatter) {
    return new QueryCompilerPG(this, builder, formatter);
  }

  columnCompiler() {
    return new ColumnCompiler(this, ...arguments);
  }

  schemaCompiler() {
    return new SchemaCompiler(this, ...arguments);
  }

  tableCompiler() {
    return new TableCompiler(this, ...arguments);
  }

  _driver() {
    return require('pg');
  }

  wrapIdentifierImpl(value) {
    if (value === '*') return value;

    let arrayAccessor = '';
    const arrayAccessorMatch = value.match(/(.*?)(\[[0-9]+\])/);

    if (arrayAccessorMatch) {
      value = arrayAccessorMatch[1];
      arrayAccessor = arrayAccessorMatch[2];
    }

    return `"${value.replace(/"/g, '""')}"${arrayAccessor}`;
  }

  // Get a raw connection, called by the `pool` whenever a new
  // connection needs to be added to the pool.
  acquireRawConnection() {
    const client = this;
    return new Promise(function (resolver, rejecter) {
      const connection = new client.driver.Client(client.connectionSettings);
      connection.connect(function (err, connection) {
        if (err) {
          return rejecter(err);
        }
        connection.on('error', (err) => {
          connection.__knex__disposed = err;
        });
        connection.on('end', (err) => {
          connection.__knex__disposed = err || 'Connection ended unexpectedly';
        });
        if (!client.version) {
          return client.checkVersion(connection).then(function (version) {
            client.version = version;
            resolver(connection);
          });
        }
        resolver(connection);
      });
    }).then(function setSearchPath(connection) {
      client.setSchemaSearchPath(connection);
      return connection;
    });
  }

  // Used to explicitly close a connection, called internally by the pool
  // when a connection times out or the pool is shutdown.
  async destroyRawConnection(connection) {
    const end = promisify((cb) => connection.end(cb));
    return end();
  }

  // In PostgreSQL, we need to do a version check to do some feature
  // checking on the database.
  checkVersion(connection) {
    return new Promise(function (resolver, rejecter) {
      connection.query('select version();', function (err, resp) {
        if (err) return rejecter(err);
        resolver(/^PostgreSQL (.*?)( |$)/.exec(resp.rows[0].version)[1]);
      });
    });
  }

  // Position the bindings for the query. The escape sequence for question mark
  // is \? (e.g. knex.raw("\\?") since javascript requires '\' to be escaped too...)
  positionBindings(sql) {
    let questionCount = 0;
    return sql.replace(/(\\*)(\?)/g, function (match, escapes) {
      if (escapes.length % 2) {
        return '?';
      } else {
        questionCount++;
        return `$${questionCount}`;
      }
    });
  }

  setSchemaSearchPath(connection, searchPath) {
    let path = searchPath || this.searchPath;

    if (!path) return Promise.resolve(true);

    if (!Array.isArray(path) && !isString(path)) {
      throw new TypeError(
        `knex: Expected searchPath to be Array/String, got: ${typeof path}`
      );
    }

    if (isString(path)) {
      if (path.includes(',')) {
        const parts = path.split(',');
        const arraySyntax = `[${parts
          .map((searchPath) => `'${searchPath}'`)
          .join(', ')}]`;
        this.logger.warn(
          `Detected comma in searchPath "${path}".` +
            `If you are trying to specify multiple schemas, use Array syntax: ${arraySyntax}`
        );
      }
      path = [path];
    }

    path = path.map((schemaName) => `"${schemaName}"`).join(',');

    return new Promise(function (resolver, rejecter) {
      connection.query(`set search_path to ${path}`, function (err) {
        if (err) return rejecter(err);
        resolver(true);
      });
    });
  }

  _stream(connection, obj, stream, options) {
    if (!obj.sql) throw new Error('The query is empty');

    const PGQueryStream = process.browser
      ? undefined
      : require('pg-query-stream');
    const sql = obj.sql;

    return new Promise(function (resolver, rejecter) {
      const queryStream = connection.query(
        new PGQueryStream(sql, obj.bindings, options)
      );

      queryStream.on('error', function (error) {
        rejecter(error);
        stream.emit('error', error);
      });

      // 'end' IS propagated by .pipe, by default
      stream.on('end', resolver);
      queryStream.pipe(stream);
    });
  }

  // Runs the query on the specified connection, providing the bindings
  // and any other necessary prep work.
  _query(connection, obj) {
    if (!obj.sql) throw new Error('The query is empty');

    let queryConfig = {
      text: obj.sql,
      values: obj.bindings || [],
    };

    if (obj.options) {
      queryConfig = extend(queryConfig, obj.options);
    }

    return new Promise(function (resolver, rejecter) {
      connection.query(queryConfig, function (err, response) {
        if (err) return rejecter(err);
        obj.response = response;
        resolver(obj);
      });
    });
  }

  // Ensures the response is returned in the same format as other clients.
  processResponse(obj, runner) {
    const resp = obj.response;
    if (obj.output) return obj.output.call(runner, resp);
    if (obj.method === 'raw') return resp;
    const { returning } = obj;
    if (resp.command === 'SELECT') {
      if (obj.method === 'first') return resp.rows[0];
      if (obj.method === 'pluck') return map(resp.rows, obj.pluck);
      return resp.rows;
    }
    if (returning) {
      const returns = [];
      for (let i = 0, l = resp.rows.length; i < l; i++) {
        const row = resp.rows[i];
        if (returning === '*' || Array.isArray(returning)) {
          returns[i] = row;
        } else {
          // Pluck the only column in the row.
          returns[i] = row[Object.keys(row)[0]];
        }
      }
      return returns;
    }
    if (resp.command === 'UPDATE' || resp.command === 'DELETE') {
      return resp.rowCount;
    }
    return resp;
  }

  async cancelQuery(connectionToKill) {
    // Error out if we can't acquire connection in time.
    // Purposely not putting timeout on `pg_cancel_backend` execution because erroring
    // early there would release the `connectionToKill` back to the pool with
    // a `KILL QUERY` command yet to finish.
    const conn = await this.acquireConnection();

    try {
      return await this._wrappedCancelQueryCall(conn, connectionToKill);
    } finally {
      // NOT returning this promise because we want to release the connection
      // in a non-blocking fashion
      this.releaseConnection(conn);
    }
  }
  _wrappedCancelQueryCall(conn, connectionToKill) {
    return this.query(conn, {
      method: 'raw',
      sql: 'SELECT pg_cancel_backend(?);',
      bindings: [connectionToKill.processID],
      options: {},
    });
  }
}

Object.assign(Client_PG.prototype, {
  dialect: 'postgresql',

  driverName: 'pg',
  canCancelQuery: true,

  _escapeBinding: makeEscape({
    escapeArray(val, esc) {
      return esc(arrayString(val, esc));
    },
    escapeString(str) {
      let hasBackslash = false;
      let escaped = "'";
      for (let i = 0; i < str.length; i++) {
        const c = str[i];
        if (c === "'") {
          escaped += c + c;
        } else if (c === '\\') {
          escaped += c + c;
          hasBackslash = true;
        } else {
          escaped += c;
        }
      }
      escaped += "'";
      if (hasBackslash === true) {
        escaped = 'E' + escaped;
      }
      return escaped;
    },
    escapeObject(val, prepareValue, timezone, seen = []) {
      if (val && typeof val.toPostgres === 'function') {
        seen = seen || [];
        if (seen.indexOf(val) !== -1) {
          throw new Error(
            `circular reference detected while preparing "${val}" for query`
          );
        }
        seen.push(val);
        return prepareValue(val.toPostgres(prepareValue), seen);
      }
      return JSON.stringify(val);
    },
  }),
});

function arrayString(arr, esc) {
  let result = '{';
  for (let i = 0; i < arr.length; i++) {
    if (i > 0) result += ',';
    const val = arr[i];
    if (val === null || typeof val === 'undefined') {
      result += 'NULL';
    } else if (Array.isArray(val)) {
      result += arrayString(val, esc);
    } else if (typeof val === 'number') {
      result += val;
    } else {
      result += JSON.stringify(typeof val === 'string' ? val : esc(val));
    }
  }
  return result + '}';
}

module.exports = Client_PG;
