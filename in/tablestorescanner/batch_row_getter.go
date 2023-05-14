package tablestorescanner

import (
	"errors"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

type batchRowGetter struct {
	client    *tablestore.TableStoreClient // 表格存储客户端
	tableName string                       // 表名

	columnsToGet []string // 只取某些字段
	columnsToDrp []string // 排除某些字段
}

func (brg *batchRowGetter) getFullRows(rows []*tablestore.Row) ([]map[string]any, error) {
	batch := 100
	uuidArr := make([]interface{}, 0, batch)
	allFlattenRows := make([]map[string]any, 0, len(rows))

	for _, row := range rows {
		for _, col := range row.PrimaryKey.PrimaryKeys {
			uuidArr = append(uuidArr, col.Value)
		}
		if len(uuidArr) >= batch {
			flattenRows, err := brg.get(uuidArr)
			if err != nil {
				return nil, err
			}
			allFlattenRows = append(allFlattenRows, flattenRows...)
			uuidArr = uuidArr[:0]
		}
	}

	if len(uuidArr) > 0 {
		flattenRows, err := brg.get(uuidArr)
		if err != nil {
			return nil, err
		}
		allFlattenRows = append(allFlattenRows, flattenRows...)
	}

	return allFlattenRows, nil
}

func (brg *batchRowGetter) get(uuids []interface{}) ([]map[string]any, error) {
	batchGetReq := &tablestore.BatchGetRowRequest{}
	mqCriteria := &tablestore.MultiRowQueryCriteria{}

	// logger.Debugf("uuids => %v", uuids)
	for _, uuid := range uuids {
		pkToGet := new(tablestore.PrimaryKey)
		pkToGet.AddPrimaryKeyColumn("uuid", uuid)
		mqCriteria.AddRow(pkToGet)
		mqCriteria.MaxVersion = 1
	}

	for _, col := range brg.columnsToGet {
		mqCriteria.AddColumnToGet(col)
	}

	mqCriteria.TableName = brg.tableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, err := brg.client.BatchGetRow(batchGetReq)
	if err != nil {
		return nil, err
	}

	flattenRows := make([]map[string]any, 0, len(uuids))
	for _, rowResult := range batchGetResponse.TableToRowsResult[brg.tableName] {
		if !rowResult.IsSucceed {
			return nil, errors.New(rowResult.Error.Code + " : " + rowResult.Error.Message)
		}
		attrs := brg.rowToMap(rowResult.PrimaryKey.PrimaryKeys, rowResult.Columns)
		// flatAttrs, _ := util.FlattenMap(attrs)
		// for _, xCol := range brg.columnsToDrp {
		// 	delete(flatAttrs, xCol)
		// 	delete(flatTypes, xCol)
		// }
		flattenRows = append(flattenRows, attrs)
	}

	return flattenRows, nil
}

func (brg *batchRowGetter) rowToMap(pks []*tablestore.PrimaryKeyColumn, cols []*tablestore.AttributeColumn) map[string]interface{} {
	attrs := make(map[string]interface{})
	for _, col := range cols {
		colName := col.ColumnName
		attrs[colName] = col.Value
	}
	for _, pkCol := range pks {
		attrs[pkCol.ColumnName] = pkCol.Value
	}
	return attrs
}
