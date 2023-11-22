# What

- I wrote this little tool to basically take anything that logs JSON to standard out (so basically all of our Control Plane services), it creates a flat key for the json object ( so Properties {foo: {bar: 0}} => becomes properties.foo.bar = 0) and shoves it into a Sqlite table, that ingests the data in realtime as long as the command is logging to standard out.
- Query with complex filtering on logs from local files, no data written to remote servers.
- Use sqlite UI as a text search engine for local logs.

# Why

- Sick of Piping, Grepping, and seeing others go line by line on JSON that is not very readable.
- Do not want to shove more temporary data into cloud...

# How

- Add sources -> Any command that logs to standard out e.g. "docker logs web -f" , "cat logs.txt", "tail logs.txt -f"
- All logs are parsed into Flat Json Key Value pairs, that are transformed in order.
- Queries can then be ran over the said sparse matrix like wide tables using sqlite as a query layer and ANY SQLITE UI of your choice!

## Query Layer:
Please download the great work by the sqlite browser team and use their wonderful UI to browse your logs.
https://sqlitebrowser.org/about/
