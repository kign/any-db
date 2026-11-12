# Java utilities to work with relational databases in database-agnostic way

Not all utilities are equally useful or equally universal (some were written for a rather narrow task),
but we preserve them here nevertheless as a reference to work respective databases.

## Installation

```bash
mvn -q -f pom.xml install
# optionally add scripts to $PATH
PATH=~/git/any-db/bin:$PATH
```

## Provided utilities

(in order of usability, from more useful to less)

Most commands have at least one required argument, which is a custom database name. 
Configuration file `~/.any-db.yaml` maps these names to common database connection info, e.g.

```yaml
connections:
  foo:
    driver: mysql          # optional, default = postgresql
    host: 111.111.111.111  # required
    port: 3306             # optional, default depends on driver
    database: clients      # required
    user: john             # optional, default = current user
    ssl: true              # optional, default = false
    alias: clients, cl     # optional, other names for the same connection (comma-separated)
    enabled: false         # optional, default = true; disabled connection (and its aliases) is ignored
```

Valid drivers are in file `SQLConnector.java` . 

Driver `gcloud-mysql` connects to MySQL in Google Cloud SQL using IAM authentication
(via [Cloud SQL socket factory](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory)),
so no password is needed. `host` is the instance connection name, and credentials are taken from
Google Application Default Credentials (ADC), normally your own account (`gcloud auth application-default login`).

`user` is either
- a full service account email: it is impersonated using ADC, and part before `@` is used as MySQL user
  (requires `roles/iam.serviceAccountTokenCreator` on that service account), or
- a MySQL user name only (IAM email truncated at `@`): the identity in ADC itself must match that user.

See [docs/gcloud_mysql.md](docs/gcloud_mysql.md) for step-by-step setup of a new service account and connection.

```yaml
connections:
  foo:
    driver: gcloud-mysql
    host: PROJECT:us-east1:SQL_INSTANCE
    database: finances
    user: sql-user@pPROJECT.iam.gserviceaccount.com
```

Passwords are **not** saved in `~/.any-db.yaml`; for that, you can use `~/.pgpass` file already used by `psql`
(Just use same exact format for non-PostgreSQL databases); this has an advantage that you can use all the
tools provided here _and_ `psql`-derived tools while keeping all passwords in one place.

You can also bypass `~/.any-db.yaml` and use options `-h` (for host), `-d` (for database name),
`-p` (for port) and `-U` (user name) to provided connection info. `~/.pgpass` is still used
for passwords.

Note also that since option `-h` is used for hist name, you'll have to invoke `--help` for command help. 

### sqli.sh

This command creates an interactive shell with some most common `psql`-like commands 
(such as `\d`) which therefore could be used with other not-PostgreSQL-compliant databases, like `MySQL`.

You can start the command with custom database name only (see above)

```bash
sqli.sh [custom database name]
```

After you start the shell, command `\h` will print the full list of commands. We also support table/column name 
completion to a limited degree.

Full history is saved in directory `~/.sqli3` (one file per database name). `log4j` logging goes to `~/sqli3.log`.

### jrun.sh

Runs either SQL inline command or SQL file. Supports parameter substitution and different output formats.

```
usage: jrun.sh [options] [custom database name] [variable=substitution] <query>
options:
 -d,--database <database>           name of database
    --default-config <CONFIG>       Default config if one not specified as argument
    --driver <DRIVER>               JDBC driver to use
 -e,--extension <EXT>               Output file extension (will cause formatted output to STDOUT)
 -f,--format <FORMAT>               Column format (e.g. '1=bold,cyan 2=red')
    --full-error                    Print full error message and Java stack on SQL exception
 -h,--host <HOST>                   redshift host name
    --help                          Print this help
 -m,--markdown <MARKDOWN FLAVOUR>   E.g. github, jira
 -M,--admin                         Assume sysadmin role (snowflake)
 -n,--dry-run                       Dry run mode (clusters should still be available)
    --no-rmi                        Inhibit RMI use (never execute in server process)
    --nosub                         No parameter substitution
    --null <NULL VALUE>             NULL value (default = NULL)
 -O,--output <FILE>                 Output file name
 -p,--port <PORT>                   port number
 -q,--query_group <QUERY_GROUP>     Query group
    --rmi                           Force RMI use (always execute in server process)
    --rs-type <RS_TYPE>             Testing only; can be 'default', 'wrapper' or 'cached'
    --single                        Treat whole file as one large query, don't attempt to split
 -t,--no-headers                    Skip header row when writing to CSV file
 -U,--user <USER>                   user name (default = current user)
 -u,--update                        Run as an 'update' query
    --unescaped                     No escaping in CSV file generation; might not open in Excel correctly, but better for line by line comparison
```

(see below for explanation of `rmi` and `--no-rmi`).

When using `-O <FILE>` option, output formatting will depend on file extension. Supported extensions
are `csv`, `xlsx` (Excel format), `txt` ot `tsv` (tab-separated values).You can also generate markdown-styled
tables with `-m` option.

### jcol.sh

Prints one or more (as long as they fit on your terminal) rows from the table in transposed format;
useful to study one such record in detail. Note that `sqli.sh` also has similar `\col` command.

### loadtodb.sh

Load CSV data to a table













