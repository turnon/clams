package mysqlloaddata

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/go-sql-driver/mysql"
	"github.com/rs/xid"
)

func init() {
	spec := service.NewConfigSpec().
		Summary("Creates an output that load data into mysql").
		Field(service.NewObjectField(
			"connect",
			service.NewStringField("host"),
			service.NewIntField("port"),
			service.NewStringField("database"),
			service.NewStringField("username"),
			service.NewStringField("password"),
		)).
		Field(service.NewStringField("table")).
		Field(service.NewStringMapField("columns")).
		Field(service.NewIntField("byte_size").Default(1024 * 1024 * 10)).
		Field(service.NewIntField("count").Default(1000)).
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
	connect, err := newMysqlloaddataConnect(conf)
	if err != nil {
		return nil, err
	}

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
		connect:  connect,
		table:    table,
		fromCols: fromCols,
		toCols:   toCols,
	}, nil
}

func newMysqlloaddataConnect(conf *service.ParsedConfig) (mysqlloaddataConnect, error) {
	host, err := conf.FieldString("connect", "host")
	if err != nil {
		return mysqlloaddataConnect{}, err
	}
	port, err := conf.FieldInt("connect", "port")
	if err != nil {
		return mysqlloaddataConnect{}, err
	}
	database, err := conf.FieldString("connect", "database")
	if err != nil {
		return mysqlloaddataConnect{}, err
	}
	username, err := conf.FieldString("connect", "username")
	if err != nil {
		return mysqlloaddataConnect{}, err
	}
	password, err := conf.FieldString("connect", "password")
	if err != nil {
		return mysqlloaddataConnect{}, err
	}
	return mysqlloaddataConnect{host: host, port: port, database: database, username: username, password: password}, nil
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
	connect mysqlloaddataConnect
	db      *sql.DB
	lock    sync.Mutex

	table         string
	localFilePath string
	loadDataCmd   string
	fromCols      []string
	toCols        []string
}

type mysqlloaddataConnect struct {
	host     string
	port     int
	database string
	username string
	password string
}

func (loaddata *mysqlloaddata) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf(
		"%s:%s@%s(%s:%d)/%s",
		loaddata.connect.username,
		loaddata.connect.password,
		"tcp",
		loaddata.connect.host,
		loaddata.connect.port,
		loaddata.connect.database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(20)
	db.SetConnMaxLifetime(0)

	if err = db.Ping(); err != nil {
		return err
	}

	loaddata.db = db

	mysql.RegisterLocalFile(loaddata.localFileName())

	return nil
}

func (loaddata *mysqlloaddata) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	loaddata.lock.Lock()
	defer loaddata.lock.Unlock()

	content, err := loaddata.generateLocalFileContent(msgs)
	if err != nil {
		return err
	}

	fileName := loaddata.localFileName()
	cmd := loaddata.loadCmd()

	if err = ioutil.WriteFile(fileName, []byte(*content), 0644); err != nil {
		return err
	}

	if _, err = loaddata.db.Exec(cmd); err != nil {
		return err
	}

	return nil
}

// loadCmd 生成loaddata命令
func (loaddata *mysqlloaddata) loadCmd() string {
	if loaddata.loadDataCmd != "" {
		return loaddata.loadDataCmd
	}

	cmd := `LOAD DATA LOCAL INFILE '%s'
	REPLACE
	INTO TABLE %s
	FIELDS TERMINATED BY ';' ENCLOSED BY '"'
	(%s)
	SET %s`

	fieldsArr := make([]string, 0, len(loaddata.toCols))
	setFiledsArr := make([]string, 0, len(loaddata.toCols))
	for _, col := range loaddata.toCols {
		field := "@" + col
		fieldsArr = append(fieldsArr, field)
		setFiledsArr = append(setFiledsArr, col+" = NULLIF("+field+", '')")
	}

	loaddata.loadDataCmd = fmt.Sprintf(cmd, loaddata.localFileName(), loaddata.table, strings.Join(fieldsArr, ", "), strings.Join(setFiledsArr, ", "))
	return loaddata.loadDataCmd
}

// localFileName 生成localfile文件名
func (loaddata *mysqlloaddata) localFileName() string {
	if loaddata.localFilePath != "" {
		return loaddata.localFilePath
	}

	guid := xid.New().String()
	fileName := strings.Join([]string{loaddata.table, guid, "csv"}, ".")
	loaddata.localFilePath = filepath.Join("/tmp", fileName)
	return loaddata.localFilePath
}

// generateLocalFileContent 生成localfile文件内容
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
	mysql.DeregisterLocalFile(loaddata.localFileName())
	os.Remove(loaddata.localFileName())
	loaddata.db.Close()
	return nil
}
