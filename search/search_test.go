package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/mock"
	//"github.com/stretchr/testify/require"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ForrestIsARealGoodman/dynamodb/client"
)

func TestNormalizeRatio(t *testing.T) {
	t.Run("NormalizeRatio with negative ratio", func(t *testing.T) {
		result := NormalizeRatio(-50)
		assert.Equal(t, 0, result)
	})

	t.Run("NormalizeRatio with ratio greater than 100", func(t *testing.T) {
		result := NormalizeRatio(150)
		assert.Equal(t, 100, result)
	})

	t.Run("NormalizeRatio with valid ratio", func(t *testing.T) {
		result := NormalizeRatio(75)
		assert.Equal(t, 75, result)
	})
}

func TestFuzzyMatchRatio(t *testing.T) {
	t.Run("FuzzyMatchRatio with empty strings", func(t *testing.T) {
		result := FuzzyMatchRatio("", "")
		assert.Equal(t, 100, result)
	})

	t.Run("FuzzyMatchRatio with one empty string", func(t *testing.T) {
		result := FuzzyMatchRatio("abc", "")
		assert.Equal(t, 0, result)
	})

	t.Run("FuzzyMatchRatio with valid strings", func(t *testing.T) {
		result := FuzzyMatchRatio("kitten", "sitting")
		assert.Equal(t, 28, result)
	})
}

func TestSearchTablesByFuzzyName(t *testing.T) {
	GetTableListClient = func(dbmgr *client.DynamoDBManager) ([]string, error) {
		return []string{"test_table_name"}, nil
	}

	GetTableArnClient = func(dbmgr *client.DynamoDBManager, tableName string) (string, error) {
		return "test_table_arn", nil
	}

	
	GetTableTagsClient = func(dbmgr *client.DynamoDBManager, tableArn string) ([]types.Tag, error) {
		mockTags := []types.Tag{
			{Key: aws.String("Tag1"), Value: aws.String("Value1")},
		}
		return mockTags, nil
	}

	t.Run("searchTablesByFuzzyName get valid table result - success", func(t *testing.T) {
		expectedResult := []map[string]string{
			{"Name": "test_table_name", "ARN": "test_table_arn"},
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		fuzzyName := "test_table"
		result := searchTablesByFuzzyName(dbmgr, fuzzyName)
		assert.ElementsMatch(t, expectedResult, result)
	})

	t.Run("searchTablesByFuzzyName get valid table result - empty results", func(t *testing.T) {
		expectedResult := []map[string]string{}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		fuzzyName := "xyz"
		result := searchTablesByFuzzyName(dbmgr, fuzzyName)
		assert.ElementsMatch(t, expectedResult, result)
	}) 
	
}

func TestSearchTablesByTagValue(t *testing.T) {
	GetTableListClient = func(dbmgr *client.DynamoDBManager) ([]string, error) {
		return []string{"test_table_name"}, nil
	}

	GetTableArnClient = func(dbmgr *client.DynamoDBManager, tableName string) (string, error) {
		return "test_table_arn", nil
	}

	
	GetTableTagsClient = func(dbmgr *client.DynamoDBManager, tableArn string) ([]types.Tag, error) {
		mockTags := []types.Tag{
			{Key: aws.String("Tag1"), Value: aws.String("Value1")},
			{Key: aws.String("Tag2"), Value: aws.String("Value2")},
		}
		return mockTags, nil
	}

	t.Run("searchTablesByTagValue get valid table result - success", func(t *testing.T) {
		expectedResult := []map[string]string{
			{"Name": "test_table_name", "ARN": "test_table_arn"},
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		tag_value := "Value1"
		var tableList []string
		result := searchTablesByTagValue(dbmgr, tag_value, tableList)
		assert.ElementsMatch(t, expectedResult, result)
	})
	
	t.Run("searchTablesByTagValue get valid table result - empty results", func(t *testing.T) {
		expectedResult := []map[string]string{}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		tag_value := "xyz"
		var tableList []string
		result := searchTablesByTagValue(dbmgr, tag_value, tableList)
		assert.ElementsMatch(t, expectedResult, result)
	}) 
}

func TestExecuteSearch(t *testing.T) {
	GetTableListClient = func(dbmgr *client.DynamoDBManager) ([]string, error) {
		return []string{"test_table_name"}, nil
	}

	GetTableArnClient = func(dbmgr *client.DynamoDBManager, tableName string) (string, error) {
		return "test_table_arn", nil
	}

	GetTableTagsClient = func(dbmgr *client.DynamoDBManager, tableArn string) ([]types.Tag, error) {
		mockTags := []types.Tag{
			{Key: aws.String("Tag1"), Value: aws.String("Value1")},
		}
		return mockTags, nil
	}
	t.Run("TestExecuteSearch with only fuzzy name", func(t *testing.T) {
		expectedResult := []map[string]string{
			{"Name": "test_table_name", "ARN": "test_table_arn"},
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		fuzzyName := "test_table"
		result := ExecuteSearch(dbmgr, fuzzyName, "")
		assert.ElementsMatch(t, expectedResult, result)
	})
	t.Run("TestExecuteSearch with only tag value", func(t *testing.T) {
		expectedResult := []map[string]string{
			{"Name": "test_table_name", "ARN": "test_table_arn"},
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		tag_value := "Value1"
		result := ExecuteSearch(dbmgr, "", tag_value)
		assert.ElementsMatch(t, expectedResult, result)
	})
	t.Run("TestExecuteSearch with both fuzzy and tag value", func(t *testing.T) {
		expectedResult := []map[string]string{
			{"Name": "test_table_name", "ARN": "test_table_arn"},
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		fuzzyName := "test_table"
		tag_value := "Value1"
		result := ExecuteSearch(dbmgr, fuzzyName, tag_value)
		assert.ElementsMatch(t, expectedResult, result)
	})
}
