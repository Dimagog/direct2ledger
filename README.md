# direct2ledger

Helps you keep your [Ledger](http://ledger-cli.org/) file up to date by doing 4 things:
1. Automatically downloads new transactions from all your banks
2. Removes duplicates and automatically classifies (assigns pair account) transactions
3. Lets you review and manually correct classification and then appends new transactions to your ledger
4. Verifies that bank-reported and Ledger balances match

## Usage

`direct2ledger journal.ldg`

Where `journal.ldg` is an existing [Ledger](http://ledger-cli.org/) file to learn from. Also `direct2ledger.yaml` file is used to store information for all your bank accounts required for automatic downloading.

Run `direct2ledger -h` for detailed usage info.

**Note** that [ledger](http://ledger-cli.org/) executable must be on your `PATH` as `direct2ledger` launches it for pre-processing of `journal.ldg`.

## Install

`go get -v -u github.com/dimagog/direct2ledger`

## Credit
Heavily based on [into-ledger](github.com/manishrjain/into-ledger).
