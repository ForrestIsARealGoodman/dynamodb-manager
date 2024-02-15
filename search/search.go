package search

// https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/gov2/dynamodb
// https://docs.aws.amazon.com/code-library/latest/ug/go_2_dynamodb_code_examples.html
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb

import (
	"strings"

	"github.com/texttheater/golang-levenshtein/levenshtein"
	"github.com/ForrestIsARealGoodman/dynamodb/client"
	"github.com/ForrestIsARealGoodman/dynamodb/logging"
)

var FUZZY_RATIO = 80
var GetTableListClient = client.GetTableList
var GetTableArnClient = client.GetTableArn
var GetTableTagsClient = client.GetTableTags


// NormalizeRatio normalizes the fuzzy ratio to be between 0 and 100.
func NormalizeRatio(ratio int) int {
	if ratio < 0 {
		return 0
	} else if ratio > 100 {
		return 100
	}
	return ratio
}

// FuzzyMatchRatio calculates the fuzzy match ratio between two strings.
func FuzzyMatchRatio(str1, str2 string) int {
	distance := levenshtein.DistanceForStrings([]rune(str1), []rune(str2), levenshtein.DefaultOptions)
	maxLen := len(str1)
	if len(str2) > maxLen {
		maxLen = len(str2)
	}

	if maxLen == 0 {
		return 100
	}

	ratio := ((maxLen - distance) * 100) / maxLen
	return ratio
}

func searchTablesByFuzzyName(dbmgr *client.DynamoDBManager, fuzzyName string) []map[string]string {
	// Get the list of table names
	tableList, err := GetTableListClient(dbmgr)
	if err != nil {
		dbmgr.Logger.Errorf("Error finding DynamoDB tables: %v", err)
		return nil
	}

	// Perform fuzzy search and filter matching tables
	matchingTables := make([]map[string]string, 0)
	for _, tableName := range tableList {
		if !strings.Contains(tableName, fuzzyName) {
			fuzzyRatio := FuzzyMatchRatio(strings.ToLower(fuzzyName), strings.ToLower(tableName))
			similarityScore := NormalizeRatio(fuzzyRatio)
			dbmgr.Logger.Debugf("Calculating: fuzzyname:%s - tablename:%s - similarityScore: %d\n", strings.ToLower(fuzzyName), strings.ToLower(tableName), similarityScore)
			if similarityScore < FUZZY_RATIO {
				continue
			}
		}
		tableArn, err := GetTableArnClient(manager, tableName)
		if err != nil {
			dbmgr.Logger.Warnf("Error getting table ARN: %v", err)
			continue
		}
		matchingTables = append(matchingTables, map[string]string{"Name": tableName, "ARN": tableArn})

	}
	return matchingTables
}

func searchTablesByTagValue(dbmgr *client.DynamoDBManager, tagValue string, tableList []string) []map[string]string {
	var matchingTables []map[string]string
	var tableListTag []string
	var errGetTable error

	if tableList != nil {
		tableListTag = make([]string, len(tableList))
		copy(tableListTag, tableList)
	} else {
		tableListTag, errGetTable = GetTableListClient(manager)
		if errGetTable != nil {
			dbmgr.Logger.Errorf("Error finding DynamoDB tables: %v", errGetTable)
			return nil
		}
	}

	// Iterate over the tableList and check tags
	for _, tableName := range tableListTag {
		// get arn
		dbmgr.Logger.Infof("Check the tags of table name: %s\n", tableName)
		tableArn, err := GetTableArnClient(manager, tableName)
		if err != nil {
			dbmgr.Logger.Warnf("Error getting table ARN: %v", err)
			continue
		}

		Tags, err_tag := GetTableTagsClient(manager, tableArn)
		if err_tag != nil {
			dbmgr.Logger.Warnf("Get tags for arn:%s, failed due to:%v", tableArn, err_tag)
			continue
	    }
		// Check if tagValue matches any tag in the list
		for _, tag := range Tags {
			dbmgr.Logger.Debugf("table_name: %s - tableArn: %s, Key: %s, Value: %s\n", tableName, tableArn, *tag.Key, *tag.Value)
			if *tag.Value == tagValue {
				matchingTables = append(matchingTables, map[string]string{"Name": tableName, "ARN": tableArn})
				break
			}
		}
	}

	return matchingTables
}

func ExecuteSearch(dbmgr *client.DynamoDBManager, tableFuzzyName string, tagValue string) []map[string]string {
	var matchingTables []map[string]string
	if tableFuzzyName != "" && tagValue != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via fuzzy name:%s, tag:%s, ...", tableFuzzyName, tagValue)
		fuzzyMatchingTables := []map[string]string{}
		fuzzyMatchingTables = searchTablesByFuzzyName(manager, tableFuzzyName)
		var tableList []string
		for _, entry := range fuzzyMatchingTables {
			name, exists := entry["Name"]
			if exists {
				tableList = append(tableList, name)
			}
		}
		matchingTables = searchTablesByTagValue(manager, tagValue, tableList)
	} else if tableFuzzyName != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via fuzzy name:%s, ...", tableFuzzyName)
		matchingTables = searchTablesByFuzzyName(manager, tableFuzzyName)
	} else if tagValue != "" {
		dbmgr.Logger.Infof("Begin to search the matched tables via tag:%s, ...", tagValue)
		matchingTables = searchTablesByTagValue(manager, tagValue, nil)
	} else {
		dbmgr.Logger.Error("Invalid search conditions: search table name or tag value should not be empty!")
		return nil
	}

	if matchingTables == nil || len(matchingTables) == 0 {
		dbmgr.Logger.Warnf("Empty search results - please check the search conditions, tableFuzzyName:%s - tagValue:%s", tableFuzzyName, tagValue)
	}

	dbmgr.Logger.Info("Search results:")
	for _, table := range matchingTables {
		dbmgr.Logger.Infof("Table Name: %s, ARN: %s\n", table["Name"], table["ARN"])
	}

	return matchingTables
}
