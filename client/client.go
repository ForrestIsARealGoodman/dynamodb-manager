package client

// https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/gov2/dynamodb
// https://docs.aws.amazon.com/code-library/latest/ug/go_2_dynamodb_code_examples.html
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	log "github.com/sirupsen/logrus"
)

var DEFAULT_RCU = 5
var DEFAULT_WCU = 5

// DynamoDBManager represents the DynamoDB manager in Go.
type DynamoDBManager struct {
	DynamoDBClient *dynamodb.Client // Add DynamoDB client
}

var LoadConfig = config.LoadDefaultConfig
var DBNewFromConfig = dynamodb.NewFromConfig
var NewListTablesPageIt = dynamodb.NewListTablesPaginator

func CreateNewDynamoDBManager(profileName string) (*DynamoDBManager, error) {
	var configToUse aws.Config
	var err error

	if profileName != "" {
		// For local test purpose
		configToUse, err = LoadConfig(context.TODO(), config.WithSharedConfigProfile(profileName))
	} else {
		// Works inside EKS pods
		// Load default configuration if custom configuration is not provided
		configToUse, err = LoadConfig(context.Background())
	}

	if err != nil {
		log.Errorf("CreateNewDynamoDBManager-config.LoadDefaultConfig:%s", err)
		return nil, errors.New("Failed to instantiate dynamoDB manager!")
	}

	return NewDynamoDBManager(configToUse)
}

func NewDynamoDBManager(cfg ...aws.Config) (*DynamoDBManager, error) {
	var configToUse aws.Config

	if len(cfg) > 0 {
		// Use the provided custom configuration
		configToUse = cfg[0]
	} else {
		return nil, errors.New("cfg must be provided!")
	}

	dbclient := DBNewFromConfig(configToUse)
	db := DynamoDBManager{
		DynamoDBClient: dbclient,
	}
	log.Debugf("Instantiated dynamoDB manager!")
	return &db, nil
}

func GetTableList(dbmgr *DynamoDBManager) ([]string, error) {
	var tableNames []string
	var output *dynamodb.ListTablesOutput
	var err error
	tablePaginator := NewListTablesPageIt(dbmgr.DynamoDBClient, &dynamodb.ListTablesInput{})
	for tablePaginator.HasMorePages() {
		output, err = tablePaginator.NextPage(context.TODO())
		if err != nil {
			log.Errorf("Couldn't list tables. Here's why: %v\n", err)
			break
		} else {
			tableNames = append(tableNames, output.TableNames...)
		}
	}
	return tableNames, err
}

func GetTableArn(dbmgr *DynamoDBManager, tableName string) (string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	output, err := dbmgr.DynamoDBClient.DescribeTable(context.TODO(), input)
	if err != nil {
		log.Errorf("Failed to get Table Arn, Here's why: %v\n", err)
		return "", err
	}
	return *output.Table.TableArn, nil

}

func GetTableTags(dbmgr *DynamoDBManager, tableArn string) ([]types.Tag, error) {
	// Initialize ListTagsOfResourceInput
	listTagsInput := &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String(tableArn),
	}

	// Call ListTagsOfResource function
	result, err := dbmgr.DynamoDBClient.ListTagsOfResource(context.TODO(), listTagsInput)
	if err != nil {
		log.Fatalf("Error calling ListTagsOfResource:%v", err)
		return nil, err
	}

	return result.Tags, nil
}

func GetCurrentBillingMode(manager *DynamoDBManager, tableName string) (string, string, string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}

	var rcu, wcu, billingMode string

	output, err := manager.DynamoDBClient.DescribeTable(context.TODO(), input)
	if err != nil {
		return "", "", "", err
	}

	if output.Table.BillingModeSummary != nil {
		billingMode = fmt.Sprintf("%v", output.Table.BillingModeSummary.BillingMode)
	}

	if billingMode == "PROVISIONED" {
		rcu = fmt.Sprintf("%d", aws.ToInt64(output.Table.ProvisionedThroughput.ReadCapacityUnits))
		wcu = fmt.Sprintf("%d", aws.ToInt64(output.Table.ProvisionedThroughput.WriteCapacityUnits))
	}

	return billingMode, rcu, wcu, nil
}

func UpdateProvisionedCapacity(manager *DynamoDBManager, switchToProvisioned bool, tableName string, rcuStr string, wcuStr string) error {
	var input *dynamodb.UpdateTableInput
	var rcuVal int64
	var wcuVal int64

	if rcuStr != "" {
		rcuVal, _ = strconv.ParseInt(rcuStr, 10, 64)
	}

	if wcuStr != "" {
		wcuVal, _ = strconv.ParseInt(wcuStr, 10, 64)
	}

	if switchToProvisioned {
		if rcuStr == "" {
			rcuVal = int64(DEFAULT_RCU)
		}

		if wcuStr == "" {
			wcuVal = int64(DEFAULT_WCU)
		}

		input = &dynamodb.UpdateTableInput{
			TableName:   &tableName,
			BillingMode: types.BillingModeProvisioned,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(rcuVal),
				WriteCapacityUnits: aws.Int64(wcuVal),
			},
		}
	} else {
		input = &dynamodb.UpdateTableInput{
			TableName: &tableName,
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(rcuVal),
				WriteCapacityUnits: aws.Int64(wcuVal),
			},
		}
	}

	_, err := manager.DynamoDBClient.UpdateTable(context.TODO(), input)
	if err != nil {
		log.Errorf("Error updating provisioned capacity: %v", err)
	} else {
		log.Infof("Provisioned capacity updated for table:%s - RCU: %d, WCU: %d", tableName, rcuVal, wcuVal)
	}

	return err
}

func SwitchToOnDemandCapacity(manager *DynamoDBManager, tableName string) error {
	input := &dynamodb.UpdateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := manager.DynamoDBClient.UpdateTable(context.TODO(), input)
	if err != nil {
		log.Errorf("error switching to on-demand capacity: %v", err)
	} else {
		log.Infof("Switched to on-demand capacity for table: %s\n", tableName)
	}

	return err
}
