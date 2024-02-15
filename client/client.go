package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ForrestIsARealGoodman/dynamodb/logging"
)

var DEFAULT_RCU = 5
var DEFAULT_WCU = 5

// DynamoDBManager represents the DynamoDB manager in Go.
type DynamoDBManager struct {
	DynamoDBClient *dynamodb.Client // Add DynamoDB client
	Logger *logging.Logger 
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
		fmt.Printf("CreateNewDynamoDBManager-config.LoadDefaultConfig:%s", err)
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
		Logger: nil,
	}
	fmt.Println("Instantiated dynamoDB manager!")
	return &db, nil
}

func SetupLogger(dbmgr *DynamoDBManager, level string) (error) {
	loggerObj, err := logging.NewLogger(level)
	if err != nil {
		//fmt.Printf("SetupLogger failed due to:%v",err)
		return err
	}
	if loggerObj == nil {
		return errors.New("Failed to setup logger, returned empty!")
	}
	dbmgr.Logger = &loggerObj
	return nil
}

func GetTableList(dbmgr *DynamoDBManager) ([]string, error) {
	var tableNames []string
	var output *dynamodb.ListTablesOutput
	var err error
	tablePaginator := NewListTablesPageIt(dbmgr.DynamoDBClient, &dynamodb.ListTablesInput{})
	for tablePaginator.HasMorePages() {
		output, err = tablePaginator.NextPage(context.TODO())
		if err != nil {
			dbmgr.Logger.Errorf("Couldn't list tables. Here's why: %v\n", err)
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
		dbmgr.Logger.Errorf("Failed to get Table Arn, Here's why: %v\n", err)
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
		dbmgr.Logger.Errorf("Error calling ListTagsOfResource:%v", err)
		return nil, err
	}

	return result.Tags, nil
}

func GetCurrentBillingMode(dbmgr *DynamoDBManager, tableName string) (string, string, string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}

	var rcu, wcu, billingMode string

	output, err := dbmgr.DynamoDBClient.DescribeTable(context.TODO(), input)
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

func UpdateProvisionedCapacity(dbmgr *DynamoDBManager, switchToProvisioned bool, tableName string, rcuStr string, wcuStr string) error {
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

	_, err := dbmgr.DynamoDBClient.UpdateTable(context.TODO(), input)
	if err != nil {
		dbmgr.Logger.Errorf("Error updating provisioned capacity: %v", err)
	} else {
		dbmgr.Logger.Infof("Provisioned capacity updated for table:%s - RCU: %d, WCU: %d", tableName, rcuVal, wcuVal)
	}

	return err
}

func (dbmgr *DynamoDBManager) SwitchToOnDemandCapacity(tableName string) error {
	input := &dynamodb.UpdateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := dbmgr.DynamoDBClient.UpdateTable(context.TODO(), input)
	if err != nil {
		dbmgr.Logger.Errorf("error switching to on-demand capacity: %v", err)
	} else {
		dbmgr.Logger.Infof("Switched to on-demand capacity for table: %s\n", tableName)
	}

	return err
}
