# Java utilities to work with relational databases in database-agnostic way

Not all utilities are equally usedful or equally universal (some were written for a rather narrow task),
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
Configuration file `~/.any-db` maps these names to common database connection info, e.g.

```
pro : driver=mysql,host=111.111.111.111,user=john,database=clients
```

Passwords are **not** saved in `~/.any-db`; for that, you can use `~/.pgpass` file already used by `psql`
(Just use same exact format for non-PostgreSQL databases); this has an advantage that you can use all the
tools provided here _and_ `psql`-derived tools while keeping all passwords in one place.

You can also bypass `~/.any-db` and use options `-h` (for host), `-d` (for database name),
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

Full history is saved in directory `~/.sqli3` (one file per database name).

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
    --nosub                         No paramater substitution
    --null <NULL VALUE>             NULL value (default = NULL)
 -O,--output <FILE>                 Output file name
 -p,--port <PORT>                   port number
 -q,--query_group <QUERY_GROUP>     Query group
    --rmi                           Force RMI use (always execute in server process)
    --rs-type <RS_TYPE>             Testing only; can be 'default', 'wrapper' or 'cached'
    --single                        Treat whole file as one large query, don't attempt to split
 -t,--no-headers                    Skip header row when writing to CVS file
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

### loadrun.sh

Run the query and save results to a table (new table tor append to existing one)

### loadtodb.sh

Load CSV data to a table













