package client

import (
	"testing"
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateNewDynamoDBManager(t *testing.T) {
	LoadConfig = func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (cfg aws.Config, err error) {
		return aws.Config{
			Logger:        nil,
			ConfigSources: nil,
		}, nil
	}
	
	DBNewFromConfig = func(cfg aws.Config, optFns ...func(*dynamodb.Options)) *dynamodb.Client {
		return nil
	}
	
	t.Run("CreateNewDynamoDBManager with profile name", func(t *testing.T) {
		db, err := CreateNewDynamoDBManager("test_profile")
		require.NoError(t, err)
		require.NotNil(t, db)
	})

	t.Run("CreateNewDynamoDBManager with default config", func(t *testing.T) {
		db, err := CreateNewDynamoDBManager("")
		require.NoError(t, err)
		require.NotNil(t, db)
	})

	t.Run("CreateNewDynamoDBManager failed with load config", func(t *testing.T) {
		LoadConfig = func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (cfg aws.Config, err error) {
			return aws.Config{
				Logger:        nil,
				ConfigSources: nil,
			}, errors.New("Failed Config")
		}
		db, err := CreateNewDynamoDBManager("")
		require.Error(t, err)
		require.Nil(t, db)
	})
}

func TestNewDynamoDBManager(t *testing.T) {
	LoadConfig = func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (cfg aws.Config, err error) {
		return aws.Config{
			Logger:        nil,
			ConfigSources: nil,
		}, nil
	}
	
	DBNewFromConfig = func(cfg aws.Config, optFns ...func(*dynamodb.Options)) *dynamodb.Client {
		return nil
	}

	t.Run("NewDynamoDBManager with profile name", func(t *testing.T) {
		db, err := NewDynamoDBManager(aws.Config{})
		assert.NoError(t, err)
		assert.NotNil(t, db)
	})
	t.Run("NewDynamoDBManager with default config", func(t *testing.T) {
		db, err := NewDynamoDBManager()
		assert.Error(t, err)
		assert.Nil(t, db)
	})
}

func TestGetTableList(t *testing.T) {
	// Create a mock DynamoDB manager and client

	// Inject the mock paginator into the mock client (if needed)
	// mockClient.ListTablesPaginator = your_mock_paginator

	// Define the expected result from the mock client
	//expectedResult := []string{"Table1", "Table2"}

	// Call the function being tested
	//result := []string{"Table1", "Table2"} //GetTableList(mockDBMgr)
	//err := nil

	// Assert the results
	//assert.NoError(t, err)
	//assert.ElementsMatch(t, expectedResult, result)
}
