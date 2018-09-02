# mint2ledger

Imports transactions downloaded from Mint.com into [Ledger](http://ledger-cli.org/) format.

## Usage

`mint2ledger -j journal.ldg -csv mint.csv`

Where `journal.ldg` is an existing [Ledger](http://ledger-cli.org/) file to learn from, and `mint.csv` contains the transactions exported from Mint.com.

**Note** that [ledger](http://ledger-cli.org/) executable must be on your `PATH` as `mint2ledger` launches it for pre-processing of `journal.ldg`.

## Install

`go get -v -u github.com/dimagog/mint2ledger`

## Credit
Heavily based on [into-ledger](github.com/manishrjain/into-ledger).
