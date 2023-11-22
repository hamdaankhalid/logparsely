# What
- Logging JSON but not able to parse and query for debugging.
- I have multiple tables for queries but I can't trace requests.
- Query with complex filtering on logs from local files, no data written to remote servers.

# Why

- Sick of Piping, Grepping, and seeing others go line by line on JSON that is not very readable.
- Use sqlite as a text search engine for local logs.

# How

- Set sources
- All logs are parsed into Flat Json Key Value pairs, that are  maintained on a stream.
- Queries can then be ran over the said sparse matrix like wide tables using sqlite as a query layer.
