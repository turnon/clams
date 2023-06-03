# clams

Some benthos plugins

## development

### local mode

example script

```yml
input:
  generate:
    interval: '@every 2s'
    mapping: 'root = {"a": now()}'
    count: 5

output:
  stdout: {}
```

run

```sh
go run main.go -local script.yml
```

### server mode

server config

```yml
tasklist:
  type: pg
  url: postgresql://postgres:mysecretpassword@localhost:5432/postgres

workers: 2
```

start server

```sh
go run main.go -server server.yml
```

send script

```sh
curl -F 'file=@script.yml' localhost:8080/api/v1/tasks
```