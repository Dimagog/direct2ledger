package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aclindsa/ofxgo"
)

func newRequest(bank *bank, acc *account) (ofxgo.Client, *ofxgo.Request) {
	basicClient := ofxgo.BasicClient{
		AppID:       "QWIN",
		AppVer:      "2400",
		SpecVersion: ofxgo.OfxVersion211,
	}
	client := ofxgo.GetClient(bank.URL, &basicClient)

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

	return client, &query
}

func translateAccountType(accType string) string {
	switch accType {
	case "CREDIT", "CREDITCARD":
		return "CREDITLINE"
	case "SAVING":
		return "SAVINGS"
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
	http.DefaultClient.Timeout = *timeout
	// http.DefaultTransport.(*http.Transport).MaxConnsPerHost = 1

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
		lookBack = *daysNew
		startDate = time.Now().Truncate(day).UTC()
	} else {
		lookBack = *daysOverlap
	}
	startDate = startDate.Add(-time.Duration(lookBack) * day)

	dtStart := &ofxgo.Date{Time: startDate}

	var dtEnd *ofxgo.Date
	if *month != "" {
		endDate, err := time.Parse("2006-01", *month)
		//startDate = endDate
		//dtStart = &ofxgo.Date{Time: startDate}
		checkf(err, "Invalid month format %s, expected YYYY-MM", *month)
		// first day of next month
		endDate = endDate.AddDate(0, 1, 0)
		dtEnd = &ofxgo.Date{Time: endDate}
		fmt.Printf("Requesting download start date of %s and end date of %s for account %s\n", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"), acc.Name)
	} else {
		fmt.Printf("Requesting download start date of %s for account %s\n", startDate.Format("2006-01-02"), acc.Name)
	}

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
			DtEnd:   dtEnd,
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
			DtEnd:   dtEnd,
			Include: true,
		}
		query.CreditCard = append(query.CreditCard, &statementRequest)
	default:
		log.Fatalf("Unsupported account type %s", acctType.String())
	}

	var response *ofxgo.Response
	for i := 0; i <= 5; i++ {
		if i != 0 {
			fmt.Printf("Retry #%d downloading account %s after error: %s\n", i, acc.Name, err.Error())
		}
		if *saveOFX {
			var httpResponse *http.Response
			// query.SetClientFields(client)
			// buf, err := query.Marshal()
			// checkf(err, "Marshal failed")
			// decodedStr := buf.String() // Decode buffer using UTF-8
			// println(decodedStr)
			httpResponse, err = client.RequestNoParse(query)
			if err == nil {
				fname := strings.Replace(acc.Name, ":", "_", -1) + ".ofx"
				fil, _ := os.Create(fname)
				r := io.TeeReader(httpResponse.Body, fil)
				response, err = ofxgo.ParseResponse(r)
			}
		} else {
			response, err = client.Request(query)
		}
		if err == nil {
			break
		}
	}
	checkf(err, "Error downloading account statement for %s", acc.Name)
	fmt.Printf("Downloaded %s\n", acc.Name)
	return response
}
