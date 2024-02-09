// main_test.go

package main

import (
	"bytes"
	"io/ioutil"
	"testing"
	"errors"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/ForrestIsARealGoodman/dynamodb/client"
)

func TestInitCommand(t *testing.T) {
	t.Run("ValidSearch", func(t *testing.T) {
		log.Info("TestInitCommand: ValidSearch")
		logOutput := bytes.NewBufferString("")
		rootCmd.SetOutput(logOutput)

		rootCmd.SetArgs([]string{"--search", "test_table", "--tag", "tag_value"})
		err := initCommand()
		assert.NoError(t, err)
		assert.NoError(t, viper.BindPFlags(rootCmd.PersistentFlags()))

	})
	t.Run("InvalidSearchAndUpdate", func(t *testing.T) {
		//rootCmd.PersistentFlags().clear()
		rootCmd.ResetFlags()

		logOutput := bytes.NewBufferString("")
		rootCmd.SetOutput(logOutput)

		rootCmd.SetArgs([]string{"--update", "update_value", "--search", "search_value", "--tag", "tag_value"})
		initCommand()

		out, err := ioutil.ReadAll(logOutput)
		if err != nil {
			t.Fatal(err)
		}
		//fmt.Printf("out is:%s", out)
		assert.NoError(t, err)
		assert.NoError(t, viper.BindPFlags(rootCmd.PersistentFlags()))
		// Check log output or any other assertions based on your requirements
		assert.Contains(t, string(out), "update can't be used together with search or tag")
	})
}

func TestCheckCommand(t *testing.T) {
	t.Run("ValidCommand", func(t *testing.T) {
		// Set up valid command parameters
		searchTerm = "test_table"
		tagValue = "tag_value"
		updateTable = ""
		rcuValueStr = ""
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect no errors
		err := checkCommand()
		assert.NoError(t, err)
	})

	t.Run("InvalidCommandWithoutAnyArgsError", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = ""
		tagValue = ""
		updateTable = ""
		rcuValueStr = ""
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "any of search or tag or update param must be provided")
	})
	t.Run("InvalidCommandWithConflictBetweenSearchAndUpdate", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = "search_value"
		tagValue = ""
		updateTable = "update_value"
		rcuValueStr = ""
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "update can't be used together with search or tag")
	})
	t.Run("InvalidCommandConflictWithNonSearchArgs", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = "search_value"
		tagValue = ""
		updateTable = ""
		rcuValueStr = "10"
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "search or tag cannot be used together with rcu, wcu, provisioned, ondemand")
	})
	t.Run("InvalidCommandConflictWithonDemandArgs", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = ""
		tagValue = ""
		updateTable = "update_value"
		rcuValueStr = "10"
		wcuValueStr = ""
		provisioned = false
		onDemand = true

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ondemand model does not support rcu or wcu")
	})
	t.Run("InvalidCommandUpdateWithoutProperArgs", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = ""
		tagValue = ""
		updateTable = "update_value"
		rcuValueStr = ""
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no rcu or wcu or provisioned or onDemand is provided")
	})
	t.Run("InvalidCommandWrongRCU", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = ""
		tagValue = ""
		updateTable = "update_value"
		rcuValueStr = "invalid number"
		wcuValueStr = ""
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid command line arguments: rcuValue:invalid number")
	})
	t.Run("InvalidCommandWrongWCU", func(t *testing.T) {
		// Set up invalid command parameters
		searchTerm = ""
		tagValue = ""
		updateTable = "update_value"
		rcuValueStr = ""
		wcuValueStr = "invalid number"
		provisioned = false
		onDemand = false

		// Call checkCommand and expect errors
		err := checkCommand()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid command line arguments: wcuValue:invalid number")
	})
}

func TestDumpParams(t *testing.T) {
	t.Run("DebugLogging", func(t *testing.T) {

		setupLoggers("Debug")
		// Capture log output for testing
		logOutput := bytes.NewBufferString("")
		log.SetOutput(logOutput)

		// Set up parameters
		searchTerm = "test_table"
		tagValue = "tag_value"
		updateTable = "update_table"
		rcuValueStr = "10"
		wcuValueStr = "20"
		provisioned = true
		onDemand = false

		// Call dumpParams
		dumpParams()

		// Check log output
		expectedLog := `Debug info - passed args listed here`

		assert.Contains(t, logOutput.String(), expectedLog)
	})
}

func TestSetupLoggers(t *testing.T) {
	t.Run("ValidLogLevel", func(t *testing.T) {
		// Capture log output for testing
		logOutput := bytes.NewBufferString("")
		log.SetOutput(logOutput)

		err := setupLoggers("Debug")

		// Check if log level is set to Debug
		assert.NoError(t, err)
		assert.Equal(t, log.DebugLevel, log.GetLevel())
	})

	t.Run("InvalidLogLevel", func(t *testing.T) {
		// Capture log output for testing
		logOutput := bytes.NewBufferString("")
		log.SetOutput(logOutput)

		err := setupLoggers("InvalidLogLevel")

		// Check if an error is returned for invalid log level
		assert.Error(t, err)
	})
}

func TestRun(t *testing.T) {
	t.Run("FailedToCreateDynamoDBManager", func(t *testing.T) {
		CreateDynamoDbClient = func(profileName string) (*client.DynamoDBManager, error) {
			db := client.DynamoDBManager{
				DynamoDBClient: nil,
			}
			return &db, errors.New("Failed")
		}
		err := run("Failed")
		assert.Error(t, err)
	})
	CreateDynamoDbClient = func(profileName string) (*client.DynamoDBManager, error) {
		db := client.DynamoDBManager{
			DynamoDBClient: nil,
		}
		return &db, nil
	}
	t.Run("UnknownAction", func(t *testing.T) {
		// Capture log output for testing
		// Test for unrecognized action
		err := run("UnknownAction")
		assert.Error(t, err)
	})
	t.Run("SearchAction", func(t *testing.T) {
		// Capture log output for testing
		// Test for unrecognized action
		ExecuteSearchTask = func(manager *client.DynamoDBManager, tableFuzzyName string, tagValue string) []map[string]string {
			return nil
		}

		err := run("search")
		assert.NoError(t, err)
	})
	t.Run("UpdateAction", func(t *testing.T) {
		// Capture log output for testing
		// Test for unrecognized action
		ExecuteUpdateTask = func(manager *client.DynamoDBManager, tableName string, paramRcu string, paramWcu string, switchToOnDemand bool, switchToProvisioned bool) error {
			return nil
		}
		err := run("update")
		assert.NoError(t, err)
	})
}
