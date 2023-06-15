package tablestorescanner

import (
	"context"
	"encoding/json"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/benthosdev/benthos/v4/public/service"
)

// 注册----------

var tablestoreConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that loop from tablestore").
	Field(service.NewStringField("end_point")).
	Field(service.NewStringField("instance_name")).
	Field(service.NewStringField("access_key_id")).
	Field(service.NewStringField("access_key_secret")).
	Field(service.NewStringField("table")).
	Field(service.NewStringField("index")).
	Field(service.NewStringField("column")).
	Field(service.NewStringField("ge")).
	Field(service.NewStringField("lt")).
	Field(service.NewObjectListField(
		"filters",
		service.NewStringField("column"),
		service.NewStringField("operator"),
		service.NewAnyField("value")),
	).
	Field(service.NewIntField("limit").Default(0))

func newTablestoreInput(conf *service.ParsedConfig) (service.Input, error) {
	endPoint, err := conf.FieldString("end_point")
	if err != nil {
		return nil, err
	}
	instanceName, err := conf.FieldString("instance_name")
	if err != nil {
		return nil, err
	}
	accessKeyId, err := conf.FieldString("access_key_id")
	if err != nil {
		return nil, err
	}
	accessKeySecret, err := conf.FieldString("access_key_secret")
	if err != nil {
		return nil, err
	}
	table, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}
	index, err := conf.FieldString("index")
	if err != nil {
		return nil, err
	}
	column, err := conf.FieldString("column")
	if err != nil {
		return nil, err
	}
	ge, err := conf.FieldString("ge")
	if err != nil {
		return nil, err
	}
	lt, err := conf.FieldString("lt")
	if err != nil {
		return nil, err
	}
	limit, err := conf.FieldInt("limit")
	if err != nil {
		return nil, err
	}

	// filters
	filters, err := conf.FieldObjectList("filters")
	if err != nil {
		return nil, err
	}
	filterArr := make([]tablestoreInputFilter, 0, len(filters))
	for _, filter := range filters {
		column, err := filter.FieldString("column")
		if err != nil {
			return nil, err
		}
		operator, err := filter.FieldString("operator")
		if err != nil {
			return nil, err
		}
		value, err := filter.FieldAny("value")
		if err != nil {
			return nil, err
		}
		filterArr = append(filterArr, tablestoreInputFilter{column: column, operator: operator, value: value})
	}

	return service.AutoRetryNacks(&tablestoreInput{
		endPoint:        endPoint,
		instanceName:    instanceName,
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
		table:           table,
		index:           index,
		column:          column,
		ge:              ge,
		lt:              lt,
		limit:           limit,
		filters:         filterArr,
	}), nil
}

func init() {
	err := service.RegisterInput(
		"tablestorescanner",
		tablestoreConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newTablestoreInput(conf)
		},
	)
	if err != nil {
		panic(err)
	}
}

// 实现----------

type tablestoreInput struct {
	endPoint        string
	instanceName    string
	accessKeyId     string
	accessKeySecret string

	table   string
	index   string
	column  string
	ge      string
	lt      string
	limit   int
	filters []tablestoreInputFilter

	client     *tablestore.TableStoreClient
	batchRowGt *batchRowGetter

	rowOrErr chan rowOrError
}

type tablestoreInputFilter struct {
	column   string
	operator string
	value    any
}

type rowOrError struct {
	row map[string]any
	err error
}

func (ts *tablestoreInput) Connect(ctx context.Context) error {
	ts.client = tablestore.NewClient(ts.endPoint, ts.instanceName, ts.accessKeyId, ts.accessKeySecret)

	ts.batchRowGt = &batchRowGetter{
		client:       ts.client,
		tableName:    ts.table,
		columnsToGet: []string{},
		columnsToDrp: []string{},
	}

	computeSplitsResp, err := ts.sessionId()
	if err != nil {
		return err
	}

	ts.rowOrErr = make(chan rowOrError)
	go ts.startLoop(computeSplitsResp.SessionId)

	return nil
}

func (ts *tablestoreInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	rowOrErr, ok := <-ts.rowOrErr
	if !ok {
		return nil, nil, service.ErrEndOfInput
	}
	if rowOrErr.err != nil {
		return nil, nil, rowOrErr.err
	}

	bytes, err := json.Marshal(rowOrErr.row)
	if err != nil {
		return nil, nil, rowOrErr.err
	}

	return service.NewMessage(bytes), func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (ts *tablestoreInput) Close(ctx context.Context) error {
	return nil
}

func (ts *tablestoreInput) sessionId() (*tablestore.ComputeSplitsResponse, error) {
	searchIndexSplitsOptions := tablestore.SearchIndexSplitsOptions{IndexName: ts.index}
	computeSplitsReq := &tablestore.ComputeSplitsRequest{TableName: ts.table}
	computeSplitsReq.SetSearchIndexSplitsOptions(searchIndexSplitsOptions)
	return ts.client.ComputeSplits(computeSplitsReq)
}

func (ts *tablestoreInput) makeFilters() []search.Query {
	rangeQuery := &search.RangeQuery{}
	rangeQuery.FieldName = ts.column
	rangeQuery.GTE(ts.ge)
	rangeQuery.LT(ts.lt)
	searchQueries := []search.Query{rangeQuery}

	for _, filter := range ts.filters {
		query := ts.makeFilter(filter.column, filter.operator, filter.value)
		searchQueries = append(searchQueries, query)
	}

	return searchQueries
}

func (ts *tablestoreInput) makeFilter(column string, operator string, value any) search.Query {
	if operator == "eq" {
		q := &search.TermQuery{}
		q.FieldName = column
		q.Term = value
		return q
	} else if operator == "exists" {
		q := &search.ExistsQuery{}
		q.FieldName = column
		return q
	} else if operator == "prefix" {
		q := &search.PrefixQuery{}
		q.FieldName = column
		q.Prefix = value.(string)
		return q
	} else if operator == "terms" {
		q := &search.TermsQuery{}
		q.FieldName = column
		values := value.([]any)
		anythings := make([]any, 0, len(values))
		anythings = append(anythings, values...)
		q.Terms = anythings
		return q
	} else {
		q := &search.RangeQuery{}
		q.FieldName = column
		switch operator {
		case "ge":
			q.GTE(value)
		case "gt":
			q.GT(value)
		case "le":
			q.LTE(value)
		case "lt":
			q.LT(value)
		}
		return q
	}
}

func (ts *tablestoreInput) startLoop(sessionId []byte) {
	searchQueries := ts.makeFilters()

	query := search.NewScanQuery().
		SetQuery(&search.BoolQuery{MustQueries: searchQueries}).
		SetLimit(1000)

	req := &tablestore.ParallelScanRequest{}
	req.SetTableName(ts.table).
		SetIndexName(ts.index).
		SetColumnsToGet(&tablestore.ColumnsToGet{Columns: []string{}}).
		SetScanQuery(query).
		SetTimeoutMs(30000).
		SetSessionId(sessionId)

	batchRowGt := &batchRowGetter{
		client:       ts.client,
		tableName:    ts.table,
		columnsToGet: []string{},
		columnsToDrp: []string{},
	}

	count := 0

scanloop:
	for {
		resp, err := ts.client.ParallelScan(req)
		if err != nil {
			ts.rowOrErr <- rowOrError{nil, err}
			break
		}

		maps, err := batchRowGt.getFullRows(resp.Rows)
		if err != nil {
			ts.rowOrErr <- rowOrError{nil, err}
		}
		for _, m := range maps {
			ts.rowOrErr <- rowOrError{m, nil}
			count += 1
			if ts.limit != 0 && count >= ts.limit {
				break scanloop
			}
		}

		if resp.NextToken == nil {
			break
		}
		req.SetScanQuery(query.SetToken(resp.NextToken))
	}

	close(ts.rowOrErr)
}
