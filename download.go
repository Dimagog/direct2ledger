package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/dimagog/ofxgo"
)

func newRequest(bank *bank, acc *account) (*ofxgo.Client, *ofxgo.Request) {
	var client = ofxgo.Client{
		AppID:  "QWIN",
		AppVer: "2400",
	}

	var query ofxgo.Request
	query.URL = bank.URL

	clientID := ofxgo.UID(bank.ClientID)
	if clientID == "" {
		id, err := ofxgo.RandomUID()
		checkf(err, "Cannot generate UID")
		clientID = *id
	}

	query.Signon.ClientUID = clientID
	query.Signon.UserID = ofxgo.String(bank.Username)
	query.Signon.UserPass = ofxgo.String(getPassword(bank))
	query.Signon.Org = ofxgo.String(bank.Org)
	query.Signon.Fid = ofxgo.String(strconv.Itoa(bank.FID))

	return &client, &query
}

func translateAccountType(accType string) string {
	switch accType {
	case "CREDIT", "CREDITCARD":
		return "CREDITLINE"
	default:
		return accType
	}
}

func readOFX(fileName string) *ofxgo.Response {
	file, err := os.Open(fileName)
	checkf(err, "Cannot open OFX file %s", fileName)
	defer file.Close()
	response, err := ofxgo.ParseResponse(file)
	checkf(err, "Cannot parse OFX file %s", fileName)
	return response
}

func downloadOFX(bank *bank, acc *account) *ofxgo.Response {
	client, query := newRequest(bank, acc)

	acctType, err := ofxgo.NewAcctType(translateAccountType(acc.Type))
	checkf(err, "Error parsing account type: %s", acc.Type)

	uid, err := ofxgo.RandomUID()
	checkf(err, "Error creating UID for transaction")

	accInfo := accountsInfo[acc.Name]

	const day = 24 * time.Hour
	var lookBack int
	startDate := accInfo.latestTxnWithFITID
	if startDate.IsZero() {
		lookBack = 30
		startDate = time.Now().Truncate(day).UTC()
	} else {
		lookBack = 12
	}
	startDate = startDate.Add(-time.Duration(lookBack) * day)

	fmt.Printf("Requesting download start date of %v for account %s\n", startDate, acc.Name)
	dtStart := &ofxgo.Date{Time: startDate}
	// endDate := time.Date(2018, 10, 1, 0, 0, 0, 0, time.UTC)
	// dtEnd := &ofxgo.Date{Time: endDate}

	switch acctType {
	case ofxgo.AcctTypeChecking, ofxgo.AcctTypeSavings:
		statementRequest := ofxgo.StatementRequest{
			TrnUID: *uid,
			BankAcctFrom: ofxgo.BankAcct{
				BankID:   ofxgo.String(bank.BankID),
				AcctID:   ofxgo.String(acc.AcctID),
				AcctType: acctType,
			},
			DtStart: dtStart,
			// DtEnd:   dtEnd,
			Include: true,
		}
		query.Bank = append(query.Bank, &statementRequest)
	case ofxgo.AcctTypeCreditLine:
		statementRequest := ofxgo.CCStatementRequest{
			TrnUID: *uid,
			CCAcctFrom: ofxgo.CCAcct{
				AcctID: ofxgo.String(acc.AcctID),
			},
			DtStart: dtStart,
			// DtEnd:   dtEnd,
			Include: true,
		}
		query.CreditCard = append(query.CreditCard, &statementRequest)
	default:
		log.Fatalf("Unsupported account type %s", acctType.String())
	}

	response, err := client.Request(query)
	checkf(err, "Error downloading account statement for %s", acc.Name)
	return response
}
