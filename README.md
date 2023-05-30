# clams

Some benthos plugins

## development

server config

```yml
tasklist:
  type: pg
  url: postgresql://postgres:mysecretpassword@localhost:5432/postgres
```

start server mode

```sh
go run main.go -s server.yml
```