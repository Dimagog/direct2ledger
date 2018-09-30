package main

import (
	"log"
	"strconv"

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

func download(bank *bank, acc *account) *ofxgo.Response {
	client, query := newRequest(bank, acc)

	acctType, err := ofxgo.NewAcctType(translateAccountType(acc.Type))
	checkf(err, "Error parsing account type: %s", acc.Type)

	uid, err := ofxgo.RandomUID()
	checkf(err, "Error creating UID for transaction")

	switch acctType {
	case ofxgo.AcctTypeChecking, ofxgo.AcctTypeSavings:
		statementRequest := ofxgo.StatementRequest{
			TrnUID: *uid,
			BankAcctFrom: ofxgo.BankAcct{
				BankID:   ofxgo.String(bank.BankID),
				AcctID:   ofxgo.String(acc.AcctID),
				AcctType: acctType,
			},
			Include: true,
		}
		query.Bank = append(query.Bank, &statementRequest)
	case ofxgo.AcctTypeCreditLine:
		statementRequest := ofxgo.CCStatementRequest{
			TrnUID: *uid,
			CCAcctFrom: ofxgo.CCAcct{
				AcctID: ofxgo.String(acc.AcctID),
			},
			Include: true,
		}
		query.CreditCard = append(query.CreditCard, &statementRequest)
	default:
		log.Fatalf("Unsupported account type %s", acctType.String())
	}

	response, err := client.Request(query)
	checkf(err, "Error downloading account statement")
	return response
}
