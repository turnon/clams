package mysqlloaddata

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/xid"
)

func init() {
	spec := service.NewConfigSpec().
		Summary("Creates an output that load data into mysql").
		Field(service.NewStringField("table")).
		Field(service.NewStringMapField("columns")).
		Field(service.NewIntField("byte_size").Default(1024 * 1024)).
		Field(service.NewIntField("count").Default(100)).
		Field(service.NewStringField("period").Default("5s"))

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

			return bo, bp, bp.Count, nil
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
	fromCols, toCols := make([]string, 0, len(columns)), make([]string, 0, len(columns))
	for from, to := range columns {
		fromCols = append(fromCols, from)
		toCols = append(toCols, to)
	}

	return &mysqlloaddata{
		table:           table,
		localFilePrefix: xid.New().String(),
		fromCols:        fromCols,
		toCols:          toCols,
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
	table           string
	localFilePrefix string
	fromCols        []string
	toCols          []string
}

func (loaddata *mysqlloaddata) Connect(ctx context.Context) error {
	return nil
}

func (loaddata *mysqlloaddata) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	fileName := loaddata.generateLocalFileName()
	content, err := loaddata.generateLocalFileContent(msgs)
	if err != nil {
		return err
	}
	fmt.Println(fileName, len(msgs))
	fmt.Println(*content)

	return nil
}

// 生成localfile文件名
func (loaddata *mysqlloaddata) generateLocalFileName() string {
	timeNow := time.Now().Format("20060102.150405")
	fileName := strings.Join([]string{loaddata.table, loaddata.localFilePrefix, timeNow, "csv"}, ".")
	return filepath.Join("/tmp", fileName)
}

// 生成localfile文件内容
func (loaddata *mysqlloaddata) generateLocalFileContent(msgs service.MessageBatch) (*string, error) {
	content := strings.Builder{}
	values := make([]string, 0, len(loaddata.fromCols))

	for _, msg := range msgs {
		structedMsg, err := msg.AsStructured()
		msgMap := structedMsg.(map[string]any)
		if err != nil {
			return nil, err
		}
		values = values[:0]
		for _, col := range loaddata.fromCols {
			value := msgMap[col]
			var valueStr string
			if value == nil {
				valueStr = "\"\""
			} else {
				valueStr = fmt.Sprintf("\"%v\"", value)
			}
			values = append(values, valueStr)
		}
		content.WriteString(strings.Join(values, ";"))
		content.WriteString("\n")
	}

	str := content.String()
	return &str, nil
}

func (loaddata *mysqlloaddata) Close(ctx context.Context) error {
	return nil
}
