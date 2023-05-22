package clickhousebatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
	conn    driver.Conn
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
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: ckb.connect.addrs,
		Auth: clickhouse.Auth{
			Database: ckb.connect.database,
			Username: ckb.connect.username,
			Password: ckb.connect.password,
		},
		Debug:        false,
		DialTimeout:  2 * time.Second,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})

	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		return err
	}

	ckb.conn = conn

	if err := ckb.createTable(ctx); err != nil {
		return err
	}

	return nil
}

func (ckb *clickhousebatch) createTable(ctx context.Context) error {
	var ddl strings.Builder
	ddl.WriteString("create table if not exists ")
	ddl.WriteString(ckb.table.name)
	ddl.WriteString("(")

	// 字段
	lastColIdx := len(ckb.table.columns) - 1
	for idx, col := range ckb.table.columns {
		ddl.WriteString(col.name)
		ddl.WriteString(" ")
		ddl.WriteString(col.ty)
		if idx != lastColIdx {
			ddl.WriteString(", ")
		}
	}

	ddl.WriteString(")")
	ddl.WriteString("engine=")

	// 引擎
	engine := ckb.table.engine
	if engine == "" {
		engine = "MergeTree"
	}
	ddl.WriteString(engine)

	// 排序
	ddl.WriteString(" order by (")
	lastOrdIdx := len(ckb.table.order) - 1
	for idx, order := range ckb.table.order {
		ddl.WriteString(order)
		if idx != lastOrdIdx {
			ddl.WriteString(", ")
		}
	}
	ddl.WriteString(")")

	// 分区
	partitionLen := len(ckb.table.partition)
	if partitionLen > 0 {
		lastpartIdx := partitionLen - 1
		ddl.WriteString(" partition by (")
		for idx, partitionExpr := range ckb.table.partition {
			ddl.WriteString(partitionExpr)
			if idx != lastpartIdx {
				ddl.WriteString(", ")
			}
		}
		ddl.WriteString(")")
	}

	if err := ckb.conn.Exec(ctx, ddl.String()); err != nil {
		return err
	}
	return nil
}

func (ckb *clickhousebatch) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	for _, msg := range msgs {
		destrcuted, err := msg.AsStructured()
		if err != nil {
			return err
		}

		mapping := destrcuted.(map[string]any)

		for col := range mapping {
			if ckb.hasColumn(col) {
				continue
			}
			columnTypes, ok := msg.MetaGet("column_types")
			if !ok {
				return errors.New("no column_types in meta")
			}
			var columnTypeMap map[string]string
			if err := json.Unmarshal([]byte(columnTypes), &columnTypeMap); err != nil {
				return err
			}
			ty := columnTypeMap[col]
			ckb.addColumn(ctx, col, ty)
		}
	}
	fmt.Println(len(msgs))
	return nil
}

func (ckb *clickhousebatch) hasColumn(name string) bool {
	for _, col := range ckb.table.columns {
		if col.name == name {
			return true
		}
	}
	return false
}

func (ckb *clickhousebatch) addColumn(ctx context.Context, name string, ty string) (bool, error) {
	if err := ckb.conn.Exec(ctx, "alter table "+ckb.table.name+" add column if not exists `"+name+"` "+ty); err != nil {
		return false, err
	}

	ckb.table.columns = append(ckb.table.columns, clickhousebatchTableColumn{name: name, ty: ty})
	return true, nil
}

func (ckb *clickhousebatch) Close(ctx context.Context) error {
	return ckb.conn.Close()
}
