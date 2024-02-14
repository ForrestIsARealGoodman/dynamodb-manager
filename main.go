package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"dynamodb/client"
	"dynamodb/search"
	"dynamodb/update"
)

// Actions the program can take
const (
	Search string = "search"
	Update string = "update"
)

var CreateDynamoDbClient = client.CreateNewDynamoDBManager
var ExecuteSearchTask = search.ExecuteSearch
var ExecuteUpdateTask = update.ExecuteUpdate

var searchTerm string
var tagValue string
var updateTable string
var rcuValueStr string
var wcuValueStr string
var provisioned bool
var onDemand bool

var usageStr string = `./dynamodb_manager --search table_name [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --tag tag_value [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --search table_name --tag tag_value [--profile profile_name][--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --update table_name --rcu rcu_value --wcu wcu_value [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --update table_name --provisioned [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --update table_name --ondemand [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]
./dynamodb_manager --update table_name --provisioned --rcu rcu_value --wcu wcu_value [--profile profile_name] [--level (Debug, Info, Warn, Error, Fatal)]`

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

func dumpParams() {
	log.Debugf("Debug info - passed args listed here:")
	log.Debugf("Search Term: %s\n", searchTerm)
	log.Debugf("Tag Value: %s\n", tagValue)
	log.Debugf("Update Table: %s\n", updateTable)
	log.Debugf("RCU Value: %s\n", rcuValueStr)
	log.Debugf("WCU Value: %s\n", wcuValueStr)
	log.Debugf("Provisioned: %t\n", provisioned)
	log.Debugf("On-Demand: %t\n", onDemand)
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
func run(action string) error {

	dbmgr, err := CreateDynamoDbClient(viper.GetString("profile"))
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create DynamoDB client due to: %v", err))
	}

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

// setupLoggers configure debug, info, and error loggers that write to stderr. The level
// can be used to control the verbosity of output.
func setupLoggers(level string) error {
	log.SetFormatter(&log.TextFormatter{
		DisableColors:    false,
		FullTimestamp:    true,
		TimestampFormat:  "2006-01-02T15:04:05-0700",
		PadLevelText:     true,
		QuoteEmptyFields: true,
	})
	if level == "" {
		log.SetLevel(log.InfoLevel)
	} else if logLevel, err := log.ParseLevel(level); err == nil {
		log.SetLevel(logLevel)
	} else {
		return errors.New("failed to configure logging for the program")
	}
	return nil
}

// main invokes the program's workflow and handles errors by returning an exit status of 1.
func main() {
	err_cmd := initCommand()
	if err_cmd != nil {
		log.Errorf("%v", err_cmd)
		os.Exit(1)
	}

	err_log := setupLoggers(viper.GetString("level"))
	if err_log != nil {
		log.Warnf("Failed to set the log level due to: %v", err_log)
	}

	dumpParams()

	if viper.GetString("update") != "" {
		err := run("update")
		if err != nil {
			log.Errorf("Failed to update the dynamodb table:%s , due to: %v", viper.GetString("update"), err)
		}
	} else {
		err := run("search")
		if err != nil {
			log.Errorf("Failed to search dynamodb table due to: %v", err)
		}
	}
	os.Exit(1)
}
