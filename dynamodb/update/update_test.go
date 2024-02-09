package update

import (
	"testing"
	"github.com/ForrestIsARealGoodman/dynamodb/client"
	"github.com/stretchr/testify/require"
)

func TestExecuteUpdate(t *testing.T) {
	GetCurrentBillingModeClient = func(manager *client.DynamoDBManager, tableName string) (string, string, string, error) {
		return "PROVISIONED", "10", "10", nil
	}

	UpdateProvisionedCapacityClient = func(manager *client.DynamoDBManager, switchToProvisioned bool, tableName string, rcuStr string, wcuStr string) error {
		return nil
	}

	SwitchToOnDemandCapacityClient = func(manager *client.DynamoDBManager, tableName string) error {
		return nil
	}
	t.Run("TestExecuteUpdate - switchToOnDemand", func(t *testing.T) {
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		table_name := "test_table"
		paramRcu := ""
		paramWcu := ""
		switchToOnDemand := true
		switchToProvisioned := false
		err := ExecuteUpdate(dbmgr, table_name, paramRcu, paramWcu, switchToOnDemand, switchToProvisioned)
		require.NoError(t, err)
	})
	t.Run("TestExecuteUpdate - switchToProvisioned", func(t *testing.T) {
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		table_name := "test_table"
		paramRcu := ""
		paramWcu := ""
		switchToOnDemand := false
		switchToProvisioned := true
		err := ExecuteUpdate(dbmgr, table_name, paramRcu, paramWcu, switchToOnDemand, switchToProvisioned)
		require.NoError(t, err)
	})
	t.Run("TestExecuteUpdate - switchToOnDemand error", func(t *testing.T) {
		GetCurrentBillingModeClient = func(manager *client.DynamoDBManager, tableName string) (string, string, string, error) {
			return "PAY_PER_REQUEST", "10", "10", nil
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		table_name := "test_table"
		paramRcu := ""
		paramWcu := ""
		switchToOnDemand := true
		switchToProvisioned := false
		err := ExecuteUpdate(dbmgr, table_name, paramRcu, paramWcu, switchToOnDemand, switchToProvisioned)
		require.NoError(t, err)
	})
	t.Run("TestExecuteUpdate - UpdateProvisionedCapacity Success", func(t *testing.T) {
		GetCurrentBillingModeClient = func(manager *client.DynamoDBManager, tableName string) (string, string, string, error) {
			return "PROVISIONED", "10", "10", nil
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		table_name := "test_table"
		paramRcu := "5"
		paramWcu := "5"
		switchToOnDemand := false
		switchToProvisioned := false
		err := ExecuteUpdate(dbmgr, table_name, paramRcu, paramWcu, switchToOnDemand, switchToProvisioned)
		require.NoError(t, err)
	})
	t.Run("TestExecuteUpdate - UpdateProvisionedCapacity error", func(t *testing.T) {
		GetCurrentBillingModeClient = func(manager *client.DynamoDBManager, tableName string) (string, string, string, error) {
			return "PAY_PER_REQUEST", "10", "10", nil
		}
		dbmgr := &client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		table_name := "test_table"
		paramRcu := ""
		paramWcu := ""
		switchToOnDemand := false
		switchToProvisioned := false
		err := ExecuteUpdate(dbmgr, table_name, paramRcu, paramWcu, switchToOnDemand, switchToProvisioned)
		require.Error(t, err)
	})
}
