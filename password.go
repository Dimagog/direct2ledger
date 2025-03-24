package main

import (
	"fmt"
	"sync"

	"github.com/howeyc/gopass"
	"github.com/zalando/go-keyring"
)

var exclusiveOp sync.Mutex

func getPassword(bank *bank) string {
	bankName := bank.Name
	if bankName == "" {
		bankName = bank.Org
	}

	key := fmt.Sprintf("%s@%s (%d)", bank.Username, bankName, bank.FID)
	exclusiveOp.Lock()
	defer exclusiveOp.Unlock()
	pwd, err := keyring.Get("direct2ledger", key)
	if err != nil {
		pwd, err = keyring.Get("direct2ledger", key)
		if err != nil {
			pwd = inputPassword(key)
			err = keyring.Set("direct2ledger", key, pwd)
			// Saving password is best-effort
			if err != nil {
				fmt.Printf("FYI: Cannot save password: %s\n", err.Error())
			}
		}
	}
	return pwd
}

func inputPassword(key string) string {
	fmt.Printf("Enter password for %s: ", key)
	bytePassword, err := gopass.GetPasswd()
	checkf(err, "Cannot read password")
	password := string(bytePassword)
	return password
}
