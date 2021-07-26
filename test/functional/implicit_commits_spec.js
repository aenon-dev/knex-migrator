const _ = require('lodash'),
    path = require('path'),
    fs = require('fs'),
    config = require('../../config'),
    KnexMigrator = require('../../lib'),
    testUtils = require('../utils'),
    debug = require('debug')('knex-migrator:index');

let migratorConfigPath,
    migrationPath;

let knexMigrator, connection;

describe('Implicit Commits', function () {
    this.timeout(1000 * 10);

    describe('knex-migrator init', function () {
        describe('fail #1', function () {
            before(function () {
                migratorConfigPath = path.join(__dirname, '..', 'assets', 'migrations_1', 'MigratorConfig.js');
                migrationPath = path.join(__dirname, '..', 'assets', 'migrations_1');

                testUtils.writeMigratorConfig({
                    migratorConfigPath: migratorConfigPath,
                    migrationPath: migrationPath,
                    currentVersion: '1.0'
                });

                knexMigrator = new KnexMigrator({
                    knexMigratorFilePath: migrationPath
                });

                connection = testUtils.connect();

                return knexMigrator.reset();
            });

            after(function () {
                if (fs.existsSync(migratorConfigPath)) {
                    fs.unlinkSync(migratorConfigPath);
                }
            });

            it('expect full DML rollback', function () {
                return knexMigrator.init()
                    .then(function () {
                        throw new Error('init should fail');
                    })
                    .catch(function (err) {
                        err.message.should.eql('unknown');
                        return connection('users');
                    })
                    .then(function (values) {
                        // mysql or postgres table still exists, was not manually rolled back, see assets
                        values.length.should.eql(0);
                    })
                    .catch(function (err) {
                        // sqlite doesn't use autocommits inside an explicit transaction
                        err.errno.should.eql(1);
                    })
                    .finally(function () {
                        if (config.get('database:client') === 'pg') {
                            debug('Destroy connection');
                            return connection.destroy()
                                .then(function () {
                                    debug('Destroyed connection');
                                });
                        }
                    });
            });
        });

        describe('fail #2', function () {
            before(function () {
                migratorConfigPath = path.join(__dirname, '..', 'assets', 'migrations_2', 'MigratorConfig.js');
                migrationPath = path.join(__dirname, '..', 'assets', 'migrations_2');

                testUtils.writeMigratorConfig({
                    migratorConfigPath: migratorConfigPath,
                    migrationPath: migrationPath,
                    currentVersion: '1.0'
                });

                knexMigrator = new KnexMigrator({
                    knexMigratorFilePath: migrationPath
                });

                connection = testUtils.connect();

                return knexMigrator.reset();
            });

            after(function () {
                if (fs.existsSync(migratorConfigPath)) {
                    fs.unlinkSync(migratorConfigPath);
                }
            });

            it('expect full DDL/DML rollback', function () {
                return knexMigrator.init()
                    .then(function () {
                        throw new Error('init should fail');
                    })
                    .catch(function (err) {
                        err.message.should.eql('unknown');
                        return connection('users');
                    })
                    .then(function () {
                        throw new Error('users table should not exist.');
                    })
                    .catch(function (err) {
                        // table not found
                        if (config.get('database:client') === 'mysql') {
                            err.errno.should.eql(1146);
                        } else if (config.get('database:client') === 'pg') {
                            err.code.should.eql('42P01');
                        } else {
                            err.errno.should.eql(1);
                        }
                    })
                    .finally(function () {
                        if (config.get('database:client') === 'pg') {
                            debug('Destroy connection');
                            return connection.destroy()
                                .then(function () {
                                    debug('Destroyed connection');
                                });
                        }
                    });
            });
        });

        describe('success #1', function () {
            before(function () {
                migratorConfigPath = path.join(__dirname, '..', 'assets', 'migrations_3', 'MigratorConfig.js');
                migrationPath = path.join(__dirname, '..', 'assets', 'migrations_3');

                testUtils.writeMigratorConfig({
                    migratorConfigPath: migratorConfigPath,
                    migrationPath: migrationPath,
                    currentVersion: '1.0'
                });

                knexMigrator = new KnexMigrator({
                    knexMigratorFilePath: migrationPath
                });

                connection = testUtils.connect();

                return knexMigrator.reset();
            });

            after(function () {
                if (fs.existsSync(migratorConfigPath)) {
                    fs.unlinkSync(migratorConfigPath);
                }
            });

            it('expect no rollback', function () {
                return knexMigrator.init()
                    .then(function () {
                        return connection('users');
                    })
                    .then(function (values) {
                        values.length.should.eql(2);

                        return connection('migrations');
                    })
                    .then(function (values) {
                        values.length.should.eql(5);
                    })
                    .finally(function () {
                        if (config.get('database:client') === 'pg') {
                            debug('Destroy connection');
                            return connection.destroy()
                                .then(function () {
                                    debug('Destroyed connection');
                                });
                        }
                    });
            });
        });
    });

    describe('knex-migrator migrate', function () {
        describe('fail #1', function () {
            before(function () {
                migratorConfigPath = path.join(__dirname, '..', 'assets', 'migrations_4', 'MigratorConfig.js');
                migrationPath = path.join(__dirname, '..', 'assets', 'migrations_4');

                testUtils.writeMigratorConfig({
                    migratorConfigPath: migratorConfigPath,
                    migrationPath: migrationPath,
                    currentVersion: '1.0'
                });

                knexMigrator = new KnexMigrator({
                    knexMigratorFilePath: migrationPath
                });

                connection = testUtils.connect();

                return knexMigrator.reset();
            });

            after(function () {
                if (fs.existsSync(migratorConfigPath)) {
                    fs.unlinkSync(migratorConfigPath);
                }
            });

            it('expect full DDL/DML rollback', function () {
                return knexMigrator.init({skipInitCompletion: true})
                    .then(function () {
                        return connection('users');
                    })
                    .then(function (values) {
                        // from init
                        values.length.should.eql(1);
                        Object.prototype.hasOwnProperty.call(values[0], 'country').should.eql(false);

                        return knexMigrator.migrate({force: true});
                    })
                    .then(function () {
                        throw new Error('Expect error from migrate.');
                    })
                    .catch(function (err) {
                        if (config.get('database:client') === 'mysql' || config.get('database:client') === 'pg') {
                            err.message.should.eql('Ooops');
                        } else {
                            // DROP COLUMN does not exist in sqlite
                            err.code.should.eql('SQLITE_ERROR');
                        }

                        return connection('dogs');
                    })
                    .then(function () {
                        throw new Error('dogs table should not exist');
                    })
                    .catch(function (err) {
                        // table not found
                        if (config.get('database:client') === 'mysql') {
                            err.errno.should.eql(1146);
                        } else if (config.get('database:client') === 'pg') {
                            err.code.should.eql('42P01');
                        } else {
                            err.errno.should.eql(1);
                        }

                        return connection('users');
                    })
                    .then(function (values) {
                        // from init
                        values.length.should.eql(1);

                        if (config.get('database:client') === 'mysql' || config.get('database:client') === 'pg') {
                            Object.prototype.hasOwnProperty.call(values[0], 'country').should.eql(false);
                        } else {
                            Object.prototype.hasOwnProperty.call(values[0], 'country').should.eql(true);
                        }
                    })
                    .finally(function () {
                        if (config.get('database:client') === 'pg') {
                            debug('Destroy connection');
                            return connection.destroy()
                                .then(function () {
                                    debug('Destroyed connection');
                                });
                        }
                    });
            });
        });

        describe('success #1', function () {
            before(function () {
                migratorConfigPath = path.join(__dirname, '..', 'assets', 'migrations_5', 'MigratorConfig.js');
                migrationPath = path.join(__dirname, '..', 'assets', 'migrations_5');

                testUtils.writeMigratorConfig({
                    migratorConfigPath: migratorConfigPath,
                    migrationPath: migrationPath,
                    currentVersion: '1.0'
                });

                knexMigrator = new KnexMigrator({
                    knexMigratorFilePath: migrationPath
                });

                connection = testUtils.connect();

                return knexMigrator.reset();
            });

            after(function () {
                if (fs.existsSync(migratorConfigPath)) {
                    fs.unlinkSync(migratorConfigPath);
                }
            });

            it('expect no rollback', function () {
                return knexMigrator.init({skipInitCompletion: true})
                    .then(function () {
                        return knexMigrator.migrate({force: true});
                    })
                    .then(function () {
                        return connection('users');
                    })
                    .then(function (values) {
                        // from init
                        values.length.should.eql(3);
                        Object.prototype.hasOwnProperty.call(values[0], 'country').should.eql(true);

                        return connection('dogs');
                    })
                    .then(function (values) {
                        values.length.should.eql(1);
                    })
                    .finally(function () {
                        if (config.get('database:client') === 'pg') {
                            debug('Destroy connection');
                            return connection.destroy()
                                .then(function () {
                                    debug('Destroyed connection');
                                });
                        }
                    });
            });
        });
    });
});
