package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ForrestIsARealGoodman/dynamodb/client"
	"github.com/ForrestIsARealGoodman/dynamodb/search"
	"github.com/ForrestIsARealGoodman/dynamodb/update"
)

// Actions the program can take
const (
	Search string = "search"
	Update string = "update"
)

var CreateDynamoDbClient = client.CreateNewDynamoDBManager
var SetupLoggerClient = client.SetupLogger
var ExecuteSearchTask = search.ExecuteSearch
var ExecuteUpdateTask = update.ExecuteUpdate

var searchTerm string
var tagValue string
var updateTable string
var rcuValueStr string
var wcuValueStr string
var provisioned bool
var onDemand bool

var usageStr string = `./dynamodb_manager --search table_name [--profile profile_name] [--level (Debug, Info, Warn, Error)]
./dynamodb_manager --tag tag_value [--profile profile_name] [--level (Debug, Info, Warn, Error)]
./dynamodb_manager --search table_name --tag tag_value [--profile profile_name][--level (Debug, Info, Warn, Error)]
./dynamodb_manager --update table_name --rcu rcu_value --wcu wcu_value [--profile profile_name] [--level (Debug, Info, Warn, Error)]
./dynamodb_manager --update table_name --provisioned [--profile profile_name] [--level (Debug, Info, Warn, Error)]
./dynamodb_manager --update table_name --ondemand [--profile profile_name] [--level (Debug, Info, Warn, Error)]
./dynamodb_manager --update table_name --provisioned --rcu rcu_value --wcu wcu_value [--profile profile_name] [--level (Debug, Info, Warn, Error)]`

var rootCmd = &cobra.Command{
	Use:   usageStr,
	Short: "Manage DynamoDB tables with fuzzy search and update capabilities",
	Long:  "Manage DynamoDB tables with fuzzy search and update capabilities",
	RunE: func(cmd *cobra.Command, args []string) error {
		searchTerm = viper.GetString("search")
		tagValue = viper.GetString("tag")
		updateTable = viper.GetString("update")
		rcuValueStr = viper.GetString("rcu")
		wcuValueStr = viper.GetString("wcu")
		provisioned = viper.GetBool("provisioned")
		onDemand = viper.GetBool("ondemand")

		return checkCommand()
	},
}

func checkCommand() error {
	if updateTable == "" && searchTerm == "" && tagValue == "" {
		return errors.New("any of search or tag or update param must be provided!")
	}

	if (searchTerm != "" || tagValue != "") && (rcuValueStr != "" || wcuValueStr != "" || provisioned || onDemand) {
		return errors.New("Invalid command line arguments: search or tag cannot be used together with rcu, wcu, provisioned, ondemand!")
	}

	if updateTable != "" && (searchTerm != "" || tagValue != "") {
		return errors.New("Invalid command line arguments: update can't be used together with search or tag!")
	}

	if updateTable != "" && rcuValueStr == "" && wcuValueStr == "" && !provisioned && !onDemand {
		return errors.New("Invalid command line arguments: no rcu or wcu or provisioned or onDemand is provided!")
	}

	if updateTable != "" && onDemand && (rcuValueStr != "" || wcuValueStr != "") {
		return errors.New("Invalid command line arguments: ondemand model does not support rcu or wcu!")
	}

	if rcuValueStr != "" {
		_, err := strconv.ParseInt(rcuValueStr, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid command line arguments: rcuValue:%s - error:%v", rcuValueStr, err))
		}
	}

	if wcuValueStr != "" {
		_, err := strconv.ParseInt(wcuValueStr, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("Invalid command line arguments: wcuValue:%s - error:%v", wcuValueStr, err))
		}
	}
	return nil
}

func dumpParams(dbmgr *client.DynamoDBManager) {
	dbmgr.Logger.Debugf("Debug info - passed args listed here:")
	dbmgr.Logger.Debugf("Search Term: %s\n", searchTerm)
	dbmgr.Logger.Debugf("Tag Value: %s\n", tagValue)
	dbmgr.Logger.Debugf("Update Table: %s\n", updateTable)
	dbmgr.Logger.Debugf("RCU Value: %s\n", rcuValueStr)
	dbmgr.Logger.Debugf("WCU Value: %s\n", wcuValueStr)
	dbmgr.Logger.Debugf("Provisioned: %t\n", provisioned)
	dbmgr.Logger.Debugf("On-Demand: %t\n", onDemand)
}

func initCommand() error {
	rootCmd.PersistentFlags().StringP("level", "", "Info", "Setup the log level")
	rootCmd.PersistentFlags().StringP("search", "", "", "Search term for DynamoDB table names")
	rootCmd.PersistentFlags().StringP("tag", "", "", "Value of the tag for DynamoDB table search")
	rootCmd.PersistentFlags().StringP("update", "", "", "Name of the DynamoDB table to update")
	rootCmd.PersistentFlags().StringP("rcu", "", "", "Read Capacity Units")
	rootCmd.PersistentFlags().StringP("wcu", "", "", "Write Capacity Units")
	rootCmd.PersistentFlags().Bool("provisioned", false, "Provisioned capacity mode")
	rootCmd.PersistentFlags().Bool("ondemand", false, "On-Demand capacity mode")

	viper.BindPFlags(rootCmd.PersistentFlags())

	cobra.EnableCommandSorting = false
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return errors.New(fmt.Sprintf("Failed to parse command line args:%v", err))
	})

	if err := rootCmd.Execute(); err != nil {
		return errors.New(fmt.Sprintf("Failed to parse command line args:%v", err))
	}
	return nil
}

// run configures and executes program's workflow.
func run(dbmgr *client.DynamoDBManager, action string) error {
	switch action {
	case Search:
		ExecuteSearchTask(dbmgr, viper.GetString("search"), viper.GetString("tag"))
	case Update:
		ExecuteUpdateTask(dbmgr, viper.GetString("update"), viper.GetString("rcu"), viper.GetString("wcu"), viper.GetBool("ondemand"), viper.GetBool("provisioned"))
	default:
		return errors.New(fmt.Sprintf("unrecognized action provided:%s", action))
	}
	return nil
}

// main invokes the program's workflow and handles errors by returning an exit status of 1.
func main() {
	err_cmd := initCommand()
	if err_cmd != nil {
		fmt.Printf("%v", err_cmd)
		os.Exit(1)
	}

	dbmgr, err := CreateDynamoDbClient(viper.GetString("profile"))
	if err != nil {
		fmt.Printf("Failed to create DynamoDB client due to: %v", err)
		os.Exit(1)
	}

	err = SetupLoggerClient(dbmgr, viper.GetString("level"))
	if err != nil {
		fmt.Printf("SetupLogger failed due to:%v", err)
		os.Exit(1)
	}

	dumpParams(dbmgr)

	if viper.GetString("update") != "" {
		err := run(dbmgr, "update")
		if err != nil {
			dbmgr.Logger.Errorf("Failed to update the dynamodb table:%s , due to: %v", viper.GetString("update"), err)
		}
	} else {
		err := run(dbmgr, "search")
		if err != nil {
			dbmgr.Logger.Errorf("Failed to search dynamodb table due to: %v", err)
		}
	}
	os.Exit(1)
}
