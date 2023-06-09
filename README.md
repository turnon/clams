# clams

Some benthos plugins

## usage

cli options

```sh
$ go run main.go --help
  -anchor string
    	anchor definition
  -debug
    	make log level DEBUG
  -local string
    	run locally
  -server string
    	server config
```

## pipeline config

yaml anchor is supported

```yml
pro_table_store: &pro_table_store
    end_point: xxx
    instance_name: xxx
    access_key_id: xxx
    access_key_secret: xxx

mysql_cluster: &mysql_cluster
    host: yyy
    port: yyy
    database: yyy
    username: yyy
    password: yyy
```

check out [benthos configuration docs](https://www.benthos.dev/docs/configuration/about) for detail

```yml
input:
    tablestorescanner:
        <<: *pro_table_store
        table: project_view_histories
        index: index_project_view_histories
        column: visit_at
        ge: "2023-06-07 00:00:00"
        lt: "2023-06-07 00:30:00"

pipeline:
    processors:
        - jq:
            query: "{user_id, visit_at}"

output:
    mysqlloaddata:
        connect:
            <<: *mysql_cluster
        table: user_events
        columns:
            user_id: user_id
            visit_at: visit_at
```

## run in local mode

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
go run main.go -local script.yml -anchor anchor.yml
```

## run in server mode

server config

```yml
tasklist:
  type: pg
  url: postgresql://admin:secret@localhost:5432/postgres

workers: 4
```

start server

```sh
go run main.go -server server.yml -anchor anchor.yml
```

create task

```sh
curl -F 'file=@script.yml' -F 'scheduled_at=2023-12-31 00:00:00' localhost:8080/api/v1/tasks
```

cancel task

```sh
curl -X DELETE localhost:8080/api/v1/tasks/234
```