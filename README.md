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
  url: postgresql://admin:secret@localhost:5432/postgres

workers: 4
```

start server

```sh
go run main.go -server server.yml
```

create task

```sh
curl -F 'file=@script.yml' -F 'scheduled_at=2023-12-31 00:00:00' localhost:8080/api/v1/tasks
```

cancel task

```sh
curl -X DELETE localhost:8080/api/v1/tasks/234
```