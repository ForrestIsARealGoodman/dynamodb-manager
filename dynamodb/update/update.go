package update

// https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/gov2/dynamodb
// https://docs.aws.amazon.com/code-library/latest/ug/go_2_dynamodb_code_examples.html
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb

import (
	"errors"
	"fmt"

	"github.com/ForrestIsARealGoodman/dynamodb/client"

	log "github.com/sirupsen/logrus"
)

var SwitchToOnDemandCapacityClient = client.SwitchToOnDemandCapacity
var UpdateProvisionedCapacityClient = client.UpdateProvisionedCapacity
var GetCurrentBillingModeClient = client.GetCurrentBillingMode

func ExecuteUpdate(manager *client.DynamoDBManager, tableName string, paramRcu string, paramWcu string, switchToOnDemand bool, switchToProvisioned bool) error {
	billingMode, rcu, wcu, err := GetCurrentBillingModeClient(manager, tableName)
	if err != nil {
		log.Errorf("Failed to get the bbilling mode info of table:%s : as current billing mode due to error:%v", tableName, err)
		return errors.New("Failed to update the table!")
	}

	if switchToOnDemand {
		if billingMode != "PAY_PER_REQUEST" {
			return SwitchToOnDemandCapacityClient(manager, tableName)
		} else {
			log.Warn("No need to switch, as it already is on demand mode!")
			return nil
		}
	} else {
		if billingMode != "PROVISIONED" && !switchToProvisioned {
			log.Errorf("Failed to update table:%s : as current billing mode:%s - does not support modification of rcu or wcu", tableName, billingMode)
			return errors.New("Failed to update the table!")
		}

		if paramRcu == "" && paramWcu == "" {
			return UpdateProvisionedCapacityClient(manager, switchToProvisioned, tableName, "", "")
		}

		if paramRcu == "" {
			paramRcu = fmt.Sprintf("%d", client.DEFAULT_RCU)
		}

		if paramWcu == "" {
			paramWcu = fmt.Sprintf("%d", client.DEFAULT_WCU)
		}

		if paramRcu != rcu || paramWcu != wcu {
			return UpdateProvisionedCapacityClient(manager, switchToProvisioned, tableName, paramRcu, paramWcu)
		} else {
			log.Warn("No need to update, as it already is provisioned mode or remain the same rcu and wcu!")
			return nil
		}
	}
}
