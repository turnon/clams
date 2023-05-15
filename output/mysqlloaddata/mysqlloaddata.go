package mysqlloaddata

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	spec := service.NewConfigSpec().
		Summary("Creates an output that load data into mysql").
		Field(service.NewStringField("table")).
		Field(service.NewStringMapField("columns")).
		Field(service.NewIntField("byte_size").Default(0)).
		Field(service.NewIntField("count").Default(0)).
		Field(service.NewStringField("period").Default(""))

	err := service.RegisterBatchOutput(
		"mysqlloaddata",
		spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			bp, err := newBatchPolicy(conf)
			if err != nil {
				return nil, bp, 0, err
			}

			bo, err := newMysqlloaddata(conf)
			if err != nil {
				return nil, bp, 0, err
			}

			return bo, bp, 1, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

func newMysqlloaddata(conf *service.ParsedConfig) (service.BatchOutput, error) {
	table, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}
	columns, err := conf.FieldStringMap("columns")
	if err != nil {
		return nil, err
	}

	return &mysqlloaddata{
		table:   table,
		columns: columns,
	}, nil
}

func newBatchPolicy(conf *service.ParsedConfig) (service.BatchPolicy, error) {
	bo := service.BatchPolicy{}
	byteSize, err := conf.FieldInt("byte_size")
	if err != nil {
		return bo, err
	}
	bo.ByteSize = byteSize

	count, err := conf.FieldInt("count")
	if err != nil {
		return bo, err
	}
	bo.Count = count

	period, err := conf.FieldString("period")
	if err != nil {
		return bo, err
	}
	bo.Period = period

	return bo, nil
}

//------------------------------------------------------------------------------

type mysqlloaddata struct {
	table   string
	columns map[string]string
}

func (loaddata *mysqlloaddata) Connect(ctx context.Context) error {
	return nil
}

func (loaddata *mysqlloaddata) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	return nil
}

func (loaddata *mysqlloaddata) Close(ctx context.Context) error {
	return nil
}
