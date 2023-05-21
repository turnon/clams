package clickhousebatch

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	spec := service.NewConfigSpec().
		Summary("Load data into clickhouse").
		Field(service.NewObjectField(
			"connect",
			service.NewStringListField("addrs"),
			service.NewStringField("database"),
			service.NewStringField("username"),
			service.NewStringField("password"),
		)).
		Field(service.NewObjectField(
			"table",
			service.NewStringField("name"),
			service.NewObjectListField(
				"columns",
				service.NewStringField("name"),
				service.NewStringField("type"),
			),
			service.NewStringField("engine"),
			service.NewStringListField("order"),
			service.NewStringListField("partition"),
		)).
		Field(service.NewBatchPolicyField("batching"))

	err := service.RegisterBatchOutput(
		"clickhousebatch",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			batchPolicy, err := conf.FieldBatchPolicy("batching")
			if err != nil {
				return nil, batchPolicy, 0, err
			}
			if batchPolicy.Count == 0 {
				batchPolicy.Count = 1
			}
			if batchPolicy.Period == "" {
				batchPolicy.Period = "10s"
			}

			ckbConnect, err := newClickhousebatchConnect(conf)
			if err != nil {
				return nil, batchPolicy, 0, err
			}

			ckbTable, err := newClickhousebatchTable(conf)
			if err != nil {
				return nil, batchPolicy, 0, err
			}

			ckb := &clickhousebatch{
				connect: ckbConnect,
				table:   ckbTable,
			}
			return ckb, batchPolicy, batchPolicy.Count, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

func newClickhousebatchConnect(conf *service.ParsedConfig) (clickhousebatchConnect, error) {
	addrs, err := conf.FieldStringList("connect", "addrs")
	if err != nil {
		return clickhousebatchConnect{}, err
	}
	database, err := conf.FieldString("connect", "database")
	if err != nil {
		return clickhousebatchConnect{}, err
	}
	username, err := conf.FieldString("connect", "username")
	if err != nil {
		return clickhousebatchConnect{}, err
	}
	password, err := conf.FieldString("connect", "password")
	if err != nil {
		return clickhousebatchConnect{}, err
	}
	return clickhousebatchConnect{addrs: addrs, database: database, username: username, password: password}, nil
}

func newClickhousebatchTable(conf *service.ParsedConfig) (clickhousebatchTable, error) {
	// name
	name, err := conf.FieldString("table", "name")
	if err != nil {
		return clickhousebatchTable{}, err
	}

	// columns
	columns, err := conf.FieldObjectList("table", "columns")
	if err != nil {
		return clickhousebatchTable{}, err
	}
	clickhousebatchTableColumnArr := make([]clickhousebatchTableColumn, 0, len(columns))
	for _, col := range columns {
		colName, err := col.FieldString("name")
		if err != nil {
			return clickhousebatchTable{}, err
		}
		colType, err := col.FieldString("type")
		if err != nil {
			return clickhousebatchTable{}, err
		}
		clickhousebatchTableColumnArr = append(clickhousebatchTableColumnArr, clickhousebatchTableColumn{name: colName, ty: colType})
	}

	// engine
	engine, err := conf.FieldString("table", "engine")
	if err != nil {
		return clickhousebatchTable{}, err
	}

	// order
	order, err := conf.FieldStringList("table", "order")
	if err != nil {
		return clickhousebatchTable{}, err
	}

	// partition
	partition, err := conf.FieldStringList("table", "partition")
	if err != nil {
		return clickhousebatchTable{}, err
	}

	// result
	return clickhousebatchTable{
		name:      name,
		columns:   clickhousebatchTableColumnArr,
		engine:    engine,
		order:     order,
		partition: partition,
	}, nil
}

//------------------------------------------------------------------------------

type clickhousebatch struct {
	connect clickhousebatchConnect
	table   clickhousebatchTable
}

type clickhousebatchConnect struct {
	addrs    []string
	database string
	username string
	password string
}

type clickhousebatchTable struct {
	name      string
	columns   []clickhousebatchTableColumn
	engine    string
	order     []string
	partition []string
}

type clickhousebatchTableColumn struct {
	name string
	ty   string
}

func (ckb *clickhousebatch) Connect(ctx context.Context) error {
	fmt.Println(ckb)
	return nil
}

func (ckb *clickhousebatch) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	fmt.Println(len(msgs))
	return nil
}

func (ckb *clickhousebatch) Close(ctx context.Context) error {
	return nil
}
