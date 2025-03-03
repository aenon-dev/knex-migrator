const knex = require('knex'),
    Promise = require('bluebird'),
    omit = require('lodash/omit'),
    debug = require('debug')('knex-migrator:database'),
    errors = require('./errors');

/**
 * @NOTE: Knex-migrator only supports knex query builder.
 *
 * @param options
 * @returns {Knex.QueryBuilder | Knex}
 */
exports.connect = function connect(options) {
    options = options || {};
    const client = options.client;

    if (client === 'sqlite3') {
        options.useNullAsDefault = options.useNullAsDefault || false;
    }

    if (client === 'mysql') {
        options.connection.timezone = options.connection.timezone || 'UTC';
        options.connection.charset = options.connection.charset || 'utf8mb4';
        options.connection.collation = options.connection.collation || 'utf8mb4_general_ci';
    }

    if (client === 'pg') {
        options.connection.timezone = options.connection.timezone || 'UTC';
        options.connection.charset = options.connection.charset || 'UTF8';
    }

    return knex(options);
};

/**
 * If you instantiate knex, you won't know if the connection works.
 * This helper functions is used to test the connection. It's basically a "test query".
 *
 * @param connection
 * @returns {Bluebird<R> | Bluebird<any> | Promise<T>}
 */
exports.ensureConnectionWorks = (connection) => {
    return connection.raw('SELECT 1+1 as RESULT;')
        .catch((err) => {
            if (err.code === 'ENOTFOUND' || err.code === 'ETIMEDOUT' || err.code === 'EAI_AGAIN') {
                throw new errors.DatabaseError({
                    message: 'Invalid database host.',
                    help: 'Please double check your database config.',
                    err: err
                });
            }

            throw new errors.DatabaseError({
                message: err.message,
                help: 'Unknown database error',
                err: err
            });
        });
};

/**
 * @description Helper to create a transaction.
 * @param callback
 * @returns {*}
 */
module.exports.createTransaction = function (connection, callback) {
    return connection.transaction(callback);
};

/**
 * @description Helper to create the migration table.
 *
 * @TODO: https://github.com/TryGhost/knex-migrator/issues/118
 * @TODO: https://github.com/TryGhost/knex-migrator/issues/91
 * @returns {Bluebird<R> | Bluebird<any> | * | Promise<T>}
 */
exports.createMigrationsTable = function createMigrationsTable(connection) {
    return connection('migrations')
        .catch(function (err) {
            // CASE: table does not exist
            if (err.errno === 1 || err.errno === 1146) {
                debug('Creating table: migrations');

                return connection.schema.createTable('migrations', function (table) {
                    table.increments().primary();
                    table.string('name');
                    table.string('version');
                    table.string('currentVersion');
                });
            }

            throw err;
        });
};

/**
 * Knex-migrator has an inbuilt feature to create a database if it does not exist yet.
 *
 * @param dbConfig
 * @returns {*}
 */
exports.createDatabaseIfNotExist = function createDatabaseIfNotExist(dbConfig) {
    const name = dbConfig.connection.database,
        collation = dbConfig.connection.collation || 'utf8mb4_general_ci',
        charset = dbConfig.client === 'mysql'
            ? dbConfig.connection.charset || 'utf8mb4'
            : dbConfig.connection.charset || 'UTF8';

    // @NOTE: Skip, because sqlite3 is a file based database.
    if (dbConfig.client === 'sqlite3') {
        return Promise.resolve();
    } else if (dbConfig.client !== 'mysql' && dbConfig.client !== 'pg') {
        return Promise.reject(new errors.KnexMigrateError({
            message: 'Database is not supported.'
        }));
    }

    const connection = exports.connect({
        client: dbConfig.client,
        connection: omit(dbConfig.connection, ['database'])
    });

    debug('Create database', name);

    return exports.ensureConnectionWorks(connection)
        .then(function () {
            return dbConfig.client === 'mysql' 
                ? connection.raw(`CREATE DATABASE ${name} CHARACTER SET ${charset} COLLATE ${collation};`)
                : connection.raw(`CREATE DATABASE ${name} WITH TEMPLATE = template0 ENCODING = '${charset}';`);
        })
        .catch(function (err) {
            // CASE: DB exists
            if (err.errno === 1007) {
                return Promise.resolve();
            }

            throw new errors.DatabaseError({
                message: err.message,
                err: err,
                code: 'DATABASE_CREATION_FAILED'
            });
        })
        .finally(function () {
            return new Promise(function (resolve, reject) {
                connection.destroy(function (err) {
                    if (err) {
                        return reject(err);
                    }

                    debug('Destroy connection');
                    resolve();
                });
            });
        });
};

/**
 * Drops a database. Is called when you call `knex-migrator reset`.
 *
 * @param options
 * @returns {*}
 */
exports.drop = function drop(options) {
    options = options || {};

    const connection = options.connection,
        dbConfig = options.dbConfig;

    if (dbConfig.client === 'mysql' || dbConfig.client === 'pg') {
        debug('Drop database: ' + dbConfig.connection.database);
        const ifExists = dbConfig.client === 'pg' ? 'IF EXISTS' : '';

        return connection.raw(`DROP DATABASE ${ifExists} ${dbConfig.connection.database};`)
            .catch(function (err) {
                // CASE: database does not exist, skip
                if (err.errno === 1049) {
                    return Promise.resolve();
                }

                return Promise.reject(new errors.KnexMigrateError({
                    err: err
                }));
            });
    } else if (dbConfig.client === 'sqlite3') {
        // @NOTE: sqlite3 does not support "DROP DATABASE". We have to drop each table instead.
        // @NOTE: We cannot just remove the sqlite3 file, because any database connection will get invalid.
        return connection.raw('SELECT name FROM sqlite_master WHERE type="table";')
            .then(function (tables) {
                return Promise.each(tables, function (table) {
                    if (table.name === 'sqlite_sequence') {
                        debug('Skip drop table: ' + table.name);
                        return Promise.resolve();
                    }

                    debug('Drop table: ' + table.name);
                    return connection.schema.dropTableIfExists(table.name);
                });
            })
            .catch(function (err) {
                // CASE: database file was never initialised
                if (err.errno === 10) {
                    return Promise.resolve();
                }

                return Promise.reject(new errors.KnexMigrateError({
                    err: err
                }));
            });
    } else {
        return Promise.reject(new errors.KnexMigrateError({
            message: 'Database client not supported: ' + dbConfig.client
        }));
    }
};
