# Toy Sqlite Implementation in GO

This is my attempt at making a simple sqlite implementation using the go toolchain, i've used the go stdlib and a small spinoff project to create my own sql parser/lexer @ [Sql parser repository](https://github.com/hemanta212/SQL-Lexer-parser/)

## Important Resources
- Challenge outline guide: https://codecrafters.io/challenges/sqlite
- Sqlite Dot commands reference: https://www.sqlite.org/cli.html#special_commands_to_sqlite3_dot_commands_
- Sqlite schema file format info: https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
- Lexical Scanning in go By rob pike: https://www.youtube.com/watch?v=HxaD_trXwRE


## CodeCrafter's ["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).
In this challenge, you build a barebones SQLite implementation that supports
basic SQL queries like `SELECT`. Along the way, learn about
[SQLite's file format](https://www.sqlite.org/fileformat.html), how indexed data
is
[stored in B-trees](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
and more.
