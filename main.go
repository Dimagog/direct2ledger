package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/gob"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"os/exec"
	"os/user"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/aclindsa/ofxgo"
	"github.com/boltdb/bolt"
	"github.com/c-bata/go-prompt"
	"github.com/eiannone/keyboard"
	"github.com/fatih/color"
	"github.com/jbrukh/bayesian"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	mathex "github.com/pkg/math"
	yaml "gopkg.in/yaml.v2"
)

func homeDir() string {
	currentUser, err := user.Current()
	if err != nil {
		return ""
	}
	return currentUser.HomeDir
}

var (
	debug           = flag.Bool("debug", false, "Print additional debug information.")
	output          = flag.String("o", "out.ldg", "Journal file to write to.")
	allowDups       = flag.Bool("allowDups", false, "Don't filter out duplicate transactions.")
	tfidf           = flag.Bool("tfidf", false, "Use TF-IDF classification algorithm instead of Bayesian (works better for small ledgers, when you are just starting).")
	timeout         = flag.Duration("timeout", 20*time.Second, "Timeout while waiting for a response from bank server.")
	noBalancesCheck = flag.Bool("noBalancesCheck", false, "Disable automatic verification that downloaded bank-reported and ledger balances match.")
	saveOFX         = flag.Bool("saveOFX", false, "Save downloaded OFX data to <account name>.ofx files.")
	noTS            = flag.Bool("noTS", false, "No timestamp comment added to output journal file.")
	daysNew         = flag.Int("daysNew", 30,
		"Download this many last `days` for NEW accounts ONLY.\n"+
			"Used when there are no ledger transactions with FITID for an account and auto-calculating download start date is impossible.")
	daysOverlap = flag.Int("daysOverlap", 12,
		"Download this many extra `days` back on top of auto-detected start date.\n"+
			"Most banks completely settle all transactions in 10 days, so the default of 12 days overlap should be enough.")
	month = flag.String("month", "", "Download transactions up to and including specific year-month (format: YYYY-MM)")

	ledgerFile = ""

	stamp      = "2006/01/02"
	bucketName = []byte("txns")
	descLength = 40
	catLength  = 20

	config       appConfig
	accountsInfo = make(map[string]*accountInfo)
)

type accountInfo struct {
	latestTxnWithFITID time.Time
	balance            amountInCurr
}

type appConfig struct {
	Banks []bank
}

type bank struct {
	Name     string
	Org      string
	FID      int
	URL      string
	BankID   string
	Username string
	ClientID string
	Accounts []account
}

func (b *bank) getName() string {
	if b.Name != "" {
		return b.Name
	}
	return b.Org
}

type account struct {
	Name           string
	AcctID         string
	Type           string
	DateAdjustment time.Duration
}

type txnhash [sha256.Size]byte

type txn struct {
	// NOTE: FITIDs are not unique across FIs
	FITID       string
	Date        time.Time
	BankDesc    string
	To          string
	From        string
	Amount      float64
	Currency    string
	Done        bool
	fromJournal bool
	hash        *txnhash
}

// NOT thread-safe
func (t *txn) Hash() *txnhash {
	if t.hash == nil {
		// Have a unique key for each transaction in so we can uniquely identify and
		// persist them.
		h := sha256.New()
		// fmt.Printf("Hashing: %s\n%s\n%.32s\n%.2f\n%s",
		// 	t.getKnownAccount(), t.Date.Format(stamp), t.BankDesc, t.Amount, t.Currency)
		desc := t.BankDesc
		const maxHashableDesc = 30
		if len(desc) > maxHashableDesc {
			desc = strings.Trim(desc[:maxHashableDesc], " \t")
		}
		fmt.Fprintf(h, "%s\n%s\n%.30s\n%.2f\n%s",
			t.getKnownAccount(), t.Date.Format(stamp), desc, t.Amount, t.Currency)
		t.hash = &txnhash{}
		h.Sum(t.hash[:0])
	}
	return t.hash
}

func (t *txn) isFromJournal() bool {
	return t.fromJournal
}

func (t *txn) getKnownAccount() string {
	if t.fromJournal || t.Amount >= 0 {
		return t.To
	}
	return t.From
}

func (t *txn) isToKnown() bool {
	return t.Amount >= 0
}

func (t *txn) isFromKnown() bool {
	return t.Amount < 0
}

func (t *txn) getPairAccount() string {
	if t.Amount >= 0 {
		return t.From
	}
	return t.To
}

func (t *txn) getKnownAccount2() (prefix, pair string) {
	if t.Amount >= 0 {
		return "[TO]", t.To
	}
	return "[FROM]", t.From
}

func (t *txn) getPairAccount2() (prefix, pair string) {
	if t.Amount >= 0 {
		return "[FROM]", t.From
	}
	return "[TO]", t.To
}

func (t *txn) setKnownAccount(acc string) {
	if t.Amount >= 0 {
		t.To = acc
	} else {
		t.From = acc
	}
}

func (t *txn) setPairAccount(pair string) {
	if t.Amount >= 0 {
		t.From = pair
	} else {
		t.To = pair
	}
}

type byTime []*txn

func (b byTime) Len() int          { return len(b) }
func (b byTime) Swap(i int, j int) { b[i], b[j] = b[j], b[i] }
func (b byTime) Less(i int, j int) bool {
	if b[i].Date.Before(b[j].Date) {
		return true
	}
	if b[i].Date.After(b[j].Date) {
		return false
	}

	ai := b[i].getKnownAccount()
	aj := b[j].getKnownAccount()

	if ai < aj {
		return true
	}
	if ai > aj {
		return false
	}

	return b[i].FITID < b[j].FITID
}

func checkf(err error, format string, args ...interface{}) {
	if err != nil {
		log.Printf(format, args...)
		log.Fatalf("%+v", errors.WithStack(err))
	}
}

func assertf(ok bool, format string, args ...interface{}) {
	if !ok {
		log.Fatalf(format, args...)
	}
}

type parser struct {
	db       *bolt.DB
	classes  []bayesian.Class
	cl       *bayesian.Classifier
	accounts []string

	// A bag of transactions.
	// It's not a set because transaction with same fields
	// can happen multiple times.
	knownTxns map[txnhash]int

	// A set of known transaction IDs (as reported by FI)
	// NOTE: FITIDs are not unique accross FIs,
	// so reported FITID is prepended with account name.
	knownTIDs     map[string]bool
	skippedFITIDs int32 // Add counter for FITID-based skips
	// map from exact transaction description to account name
	alwaysAutoAccept map[string]string
}

func newParser(db *bolt.DB) parser {
	return parser{
		db:               db,
		knownTxns:        make(map[txnhash]int),
		knownTIDs:        make(map[string]bool),
	}
}

var reFITIDtag = regexp.MustCompile(`\WFITID:\s*(\S+)`)

func parseJournalTransaction(cols []string) *txn {
	t := txn{fromJournal: true}

	var err error
	t.Date, err = time.Parse(stamp, cols[0])
	checkf(err, "Unable to parse time: %v", cols[0])

	// TODO: Decide if ledger descriptions should be cleaned up
	// t.BankDesc = strings.Trim(cols[2], " \n\t")
	t.BankDesc = cleanupDesc(cols[2])

	t.To = cols[3]
	assertf(len(t.To) > 0, "Expected TO, found empty.")

	t.Currency = cols[4]
	t.Amount, err = strconv.ParseFloat(cols[5], 64)
	checkf(err, "Unable to parse amount.")

	{
		meta := cols[7]
		matches := reFITIDtag.FindStringSubmatch(meta)
		assertf(len(matches) <= 2, "Multiple FITIDs: %s", meta)
		if len(matches) == 2 {
			t.FITID = matches[1]
		}
	}
	return &t
}

func parseJournalTransactions(tch chan<- *txn) {
	const errText = "Unable to convert journal to csv. Possibly an issue with your ledger installation."

	defer close(tch)

	cmd := exec.Command("ledger", "-f", ledgerFile, "csv")
	out, err := cmd.StdoutPipe()
	checkf(err, errText)
	err = cmd.Start()
	checkf(err, errText)

	r := csv.NewReader(newConverter(out))
	for {
		cols, err := r.Read()
		if err == io.EOF {
			break
		}
		checkf(err, "Unable to read a csv line.")

		tch <- parseJournalTransaction(cols)
	}

	err = cmd.Wait()
	checkf(err, errText)
}

const autoAcceptFile = "direct2ledger.autoaccept"

func (p *parser) readAutoAccepts() {
	p.alwaysAutoAccept = make(map[string]string)

	file, err := os.Open(autoAcceptFile)
	if err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) == 2 {
				p.alwaysAutoAccept[parts[0]] = parts[1]
			} else {
				log.Printf("Invalid autoaccept line: %v", line)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading autoaccept file: %v", err)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("Error opening autoaccept file: %v", err)
	}
}

func (p *parser) writeAutoAccepts() {
	if len(p.alwaysAutoAccept) == 0 {
		return
	}

	file, err := os.Create(autoAcceptFile)
	if err != nil {
		log.Printf("Error creating autoaccept file: %v", err)
		return
	}
	defer file.Close()

	// Sort keys for deterministic output
	keys := slices.Collect(maps.Keys(p.alwaysAutoAccept))
	sort.Strings(keys)

	for _, desc := range keys {
		acc := p.alwaysAutoAccept[desc]
		if _, err := fmt.Fprintf(file, "%s\t%s\n", desc, acc); err != nil {
			log.Printf("Error writing autoaccept file: %v", err)
			return
		}
	}
}

func (p *parser) readAccounts() {
	const errText = "Unable to extract accounts from journal. Possibly an issue with your ledger installation."

	cmd := exec.Command("ledger", "-f", ledgerFile, "accounts")
	out, err := cmd.StdoutPipe()
	checkf(err, errText)

	err = cmd.Start()
	checkf(err, errText)

	s := bufio.NewScanner(out)
	for s.Scan() {
		acc := s.Text()
		if acc != "" {
			p.accounts = append(p.accounts, acc)
		}
	}

	err = cmd.Wait()
	checkf(err, errText)
}

func (p *parser) prepareTraining() {
	p.classes = make([]bayesian.Class, 0, len(p.accounts))
	for _, acc := range p.accounts {
		p.classes = append(p.classes, bayesian.Class(acc))
	}
	assertf(len(p.classes) > 1, "Expected some categories. Found none.")

	if *tfidf {
		p.cl = bayesian.NewClassifierTfIdf(p.classes...)
	} else {
		p.cl = bayesian.NewClassifier(p.classes...)
	}
	assertf(p.cl != nil, "Expected a valid classifier. Found nil.")
}

func (p *parser) train(t *txn) {
	p.cl.Learn(t.getTerms(), bayesian.Class(t.To))
	if !*allowDups {
		p.knownTxns[*t.Hash()]++
	}
	if t.FITID != "" {
		tid := formatTid(t.To, t.FITID)
		p.knownTIDs[tid] = true

		accInfo := accountsInfo[t.To]
		if accInfo != nil {
			latestDate := accInfo.latestTxnWithFITID
			if t.Date.After(latestDate) {
				accInfo.latestTxnWithFITID = t.Date
			}
		}
	}
}

func (p *parser) finishTraining() {
	if *tfidf {
		p.cl.ConvertTermsFreqToTfIdf()
	}
}

type pair struct {
	score float64
	pos   int
}

type byScore []pair

func (b byScore) Len() int {
	return len(b)
}
func (b byScore) Less(i int, j int) bool {
	return b[i].score > b[j].score
}
func (b byScore) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

var trimWhitespace = regexp.MustCompile(`^\s+|\s+$`)
var dedupWhitespace = regexp.MustCompile(`\s+`)

func normalizeWhitespace(s string) string {
	s = trimWhitespace.ReplaceAllString(s, "")
	s = dedupWhitespace.ReplaceAllString(s, " ") // also converts all whitespace to space
	return s
}

func (t *txn) getTerms() []string {
	desc := strings.ToUpper(t.BankDesc)
	desc = normalizeWhitespace(desc)
	terms := strings.Split(desc, " ")

	terms = append(terms, "BankDesc: "+desc)

	var amt float64
	if t.isFromJournal() {
		amt = t.Amount
		if *debug {
			fmt.Printf("Learning about account: %v\n", t.To)
		}
	} else {
		amt = -t.Amount // we are looking for the opposite
	}

	var kind string
	if amt >= 0 {
		kind = "credit"
	} else {
		kind = "debit"
	}
	terms = append(terms, "Kind: "+kind)

	terms = append(terms, "AmountClassFine: "+strconv.Itoa(getAmountClassFine(amt)))
	terms = append(terms, "AmountClassCoarse: "+strconv.Itoa(getAmountClassCoarse(amt)))

	if *debug {
		fmt.Printf("getTerms(%s, %.2f) = %v\n", t.BankDesc, t.Amount, terms)
	}

	return terms
}

func getAmountClassFine(amount float64) int {
	if amount == 0 {
		return 0
	}

	log := math.Round(math.Log10(math.Abs(amount)) * 4)
	class := int(math.Round(math.Pow(10, log/4)))
	return class
}

func getAmountClassCoarse(amount float64) int {
	if amount == 0 {
		return 0
	}

	log := int(math.Ceil(math.Log10(math.Abs(amount))))
	class := int(math.Round(math.Pow10(log)))
	return class
}

func (p *parser) topHits(t *txn) []bayesian.Class {
	terms := t.getTerms()
	scores, _, _ := p.cl.LogScores(terms)

	knownAccount := bayesian.Class(t.getKnownAccount())

	pairs := make([]pair, 0, len(scores))
	skipIndex := -1
	for pos, score := range scores {
		if p.classes[pos] == knownAccount {
			if *debug {
				fmt.Printf("Removed self '%s' at index %d with score %f\n", knownAccount, pos, score)
			}
			skipIndex = pos
		} else {
			pairs = append(pairs, pair{score, pos})
		}
	}

	if skipIndex >= 0 {
		scores = append(scores[:skipIndex], scores[skipIndex+1:]...)
	}

	var mean, stddev float64
	for _, score := range scores {
		mean += score
	}
	mean /= float64(len(scores))

	for _, score := range scores {
		stddev += math.Pow(score-mean, 2)
	}
	stddev = math.Sqrt(stddev / float64(len(scores)-1))

	if *debug {
		fmt.Printf("stddev=%f\n", stddev)
	}

	sort.Sort(byScore(pairs))
	result := make([]bayesian.Class, 0, 10)
	last := pairs[0].score
	for i := 0; i < mathex.Min(10, len(pairs)); i++ {
		pr := pairs[i]
		if math.Abs(pr.score-last) > stddev {
			break
		}
		if *debug {
			fmt.Printf("i=%d s=%.3g Class=%v\n", i, pr.score, p.classes[pr.pos])
		}
		result = append(result, p.classes[pr.pos])
		last = pr.score
	}
	return result
}

func (p *parser) downloadAndParseBankAccount(bank *bank, acc *account, tch chan<- *txn) {
	if *debug {
		fmt.Printf("Downloading %s -> %s\n", bank.getName(), acc.Name)
	}

	// resp := readOFX("test.ofx")
	resp := downloadOFX(bank, acc)

	meaning, _ := resp.Signon.Status.CodeMeaning()
	assertf(resp.Signon.Status.Code == 0, "Signon failed: %s", meaning)

	var (
		balanceAmount   ofxgo.Amount
		defaultCurrency ofxgo.CurrSymbol
		statementDate   ofxgo.Date
		transactions    []ofxgo.Transaction
	)

	if len(resp.Bank) > 0 {
		if stmt, ok := resp.Bank[0].(*ofxgo.StatementResponse); ok {
			balanceAmount = stmt.BalAmt
			defaultCurrency = stmt.CurDef
			statementDate = stmt.DtAsOf
			assertf(stmt.BankTranList != nil, "No transactions received for %s", acc.Name)
			transactions = stmt.BankTranList.Transactions
		}
	} else if len(resp.CreditCard) > 0 {
		if stmt, ok := resp.CreditCard[0].(*ofxgo.CCStatementResponse); ok {
			balanceAmount = stmt.BalAmt
			defaultCurrency = stmt.CurDef
			statementDate = stmt.DtAsOf
			assertf(stmt.BankTranList != nil, "No transactions received for %s", acc.Name)
			transactions = stmt.BankTranList.Transactions
		}
	} else {
		assertf(false, "No messages received")
	}

	if *debug {
		fmt.Printf("Balance for %s: %s %s (as of %s)\n", acc.Name, balanceAmount, defaultCurrency, statementDate)
	}
	amount, _ := balanceAmount.Float64()
	accountsInfo[acc.Name].balance = amountInCurr{amount, translateCurrency(defaultCurrency.String())}

	for _, tran := range transactions {
		t := p.parseBankTransaction(acc, defaultCurrency, &tran)
		if t != nil {
			tch <- t
		}
	}

	if *debug {
		fmt.Printf("Finished downloading %s -> %s\n", bank.getName(), acc.Name)
	}
}

func (p *parser) downloadAndParseNewTransactions(tch chan<- *txn) {
	var allDone sync.WaitGroup
	for iBank := range config.Banks {
		bank := &config.Banks[iBank]
		for iAcc := range bank.Accounts {
			acc := &bank.Accounts[iAcc]
			allDone.Add(1)
			go func() {
				p.downloadAndParseBankAccount(bank, acc, tch)
				allDone.Done()
			}()
		}
	}

	go func() {
		allDone.Wait()
		close(tch)
	}()
}

var descGarbage = regexp.MustCompile(`^((Ext Credit Card (Credit|Debit))|(Descriptive )?Withdrawal)(--)?`)

// var descRemoveSymbols = regexp.MustCompile(`[#.*-]`)

func cleanupDesc(desc string) string {
	if *debug {
		fmt.Printf("Desc FROM: %s\n", desc)
	}
	d := descGarbage.ReplaceAllLiteralString(desc, "")
	if d != "" {
		desc = d
	}
	desc = strings.Replace(desc, "--", " ", 1)
	// desc = descRemoveSymbols.ReplaceAllLiteralString(desc, " ")
	desc = normalizeWhitespace(desc)
	desc = dedupDescription(desc)
	if *debug {
		fmt.Printf("Desc TO  : %s\n\n", desc)
	}
	return desc
}

func extractDescription(t *ofxgo.Transaction) string {
	var desc string
	name := t.Name.String()
	memo := t.Memo.String()

	if strings.Contains(memo, name) {
		desc = memo
	} else {
		desc = fmt.Sprintf("%s %s", name, memo)
	}
	if t.CheckNum != "" {
		desc += " " + t.CheckNum.String()
	}
	return cleanupDesc(desc)
}

func dedupDescription(origDesc string) string {
	desc := origDesc + " " // for cases like "abc abc", i.e. with space in between duplicates
outer:
	for j := len(desc) / 2; j > 3; j-- {
		for i := 0; i < j; i++ {
			if desc[i] != desc[j+i] {
				continue outer
			}
		}
		// found the longest duplicate
		if *debug {
			fmt.Printf("Deduped from: %s\n", origDesc)
			fmt.Printf("Deduped to:   %s\n", origDesc[j:])
		}
		return origDesc[j:]
	}
	return origDesc // no deduping
}

func printCategory(t *txn) {
	prefix, cat := t.getPairAccount2()
	if len(cat) == 0 {
		return
	}
	if len(cat) > catLength {
		cat = cat[len(cat)-catLength:]
	}
	color.New(color.BgHiYellow, color.FgBlack).Printf(" %6s %-20s ", prefix, cat)

	prefix, cat = t.getKnownAccount2()
	if len(cat) == 0 {
		return
	}
	if len(cat) > catLength {
		cat = cat[len(cat)-catLength:]
	}
	color.New(color.BgGreen, color.FgBlack).Printf(" %6s %-20s ", prefix, cat)
}

func printSummary(t *txn, idx, total int) {
	idx++
	if total > 0 {
		if t.Done {
			color.New(color.BgGreen, color.FgBlack).Printf(" R ")
		} else {
			color.New(color.BgRed, color.FgWhite).Printf(" N ")
		}
	}

	if total > 999 {
		color.New(color.BgBlue, color.FgWhite).Printf(" [%4d of %4d] ", idx, total)
	} else if total > 99 {
		color.New(color.BgBlue, color.FgWhite).Printf(" [%3d of %3d] ", idx, total)
	} else if total > 0 {
		color.New(color.BgBlue, color.FgWhite).Printf(" [%2d of %2d] ", idx, total)
	} else if total == 0 {
		// A bit of a hack, but will do.
		color.New(color.BgBlue, color.FgWhite).Printf(" [DUPLICATE] ")
	} else if total < 0 {
		// A bit of a hack, but will do.
		color.New(color.BgBlue, color.FgWhite).Printf(" [PENDING] ")
	} else {
		log.Fatalf("Unhandled case for total: %v", total)
	}

	color.New(color.BgYellow, color.FgBlack).Printf(" %10s ", t.Date.Format(stamp))
	desc := t.BankDesc
	if len(desc) > descLength {
		desc = desc[:descLength]
	}
	color.New(color.BgWhite, color.FgBlack).Printf(" %-40s", desc) // descLength used in Printf.
	printCategory(t)

	color.New(color.BgRed, color.FgWhite).Printf(" %9.2f %3s ", t.Amount, t.Currency)
	if *debug {
		fmt.Printf(" hash: %s", hex.EncodeToString(t.Hash()[:]))
	}
	fmt.Println()
}

func clear() {
	fmt.Println()
}

func (p *parser) writeToDB(t *txn) {
	if err := p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		var val bytes.Buffer
		enc := gob.NewEncoder(&val)
		checkf(enc.Encode(t), "Unable to encode txn: %v", t)
		return b.Put(t.Hash()[:], val.Bytes())

	}); err != nil {
		log.Fatalf("Write to db failed with error: %v", err)
	}
}

func buildCompleter(accounts []bayesian.Class) prompt.Completer {
	strs := make([]string, len(accounts))
	for i, a := range accounts {
		strs[i] = string(a)
	}
	sort.Strings(strs)

	sug := make([]prompt.Suggest, len(strs))
	for i, a := range strs {
		sug[i] = prompt.Suggest{Text: a}
	}

	return func(d prompt.Document) []prompt.Suggest {
		return prompt.FilterContains(sug, d.GetWordBeforeCursor(), true)
	}
}

func (p *parser) printAndGetResult(hits []bayesian.Class, t *txn) int {
	for {
		autoAccount, found := p.alwaysAutoAccept[t.BankDesc]
		if !t.Done && t.isFromKnown() && found && autoAccount != "" {
			color.New(color.BgGreen, color.FgBlack).Printf("Auto-accepted as: ")
			color.New(color.BgYellow, color.FgBlack).Printf(" %s ", autoAccount)
			fmt.Println()
			t.setPairAccount(autoAccount)
			t.Done = true
			p.writeToDB(t)
			return 1
		}

		for i, acc := range hits {
			fmt.Printf("%2d. %s\n", (i+1)%10, acc)
		}
		fmt.Println()

		fmt.Print("[Enter]=Accept, accept (a)ll like this, [space]=type in, (b)ack, (s)kip (q)uit or number> ")
		ch, key, _ := keyboard.GetSingleKey()
		if unicode.IsPrint(ch) {
			fmt.Printf("%c", ch)
		}
		fmt.Println()

		if (ch == 'a' || key == keyboard.KeyEnter) && len(t.To) > 0 && len(t.From) > 0 {
			t.Done = true
			p.writeToDB(t)
			if ch == 'a' && t.isFromKnown() {
				fmt.Println("Auto-accepting all transactions like this.")
				p.alwaysAutoAccept[t.BankDesc] = t.To
			}
			return 1
		}

		var account string

		if unicode.IsDigit(ch) {
			choice := int(ch - '0')
			if choice == 0 {
				choice = 10
			}
			choice--
			if choice >= len(hits) {
				fmt.Println()
				color.New(color.BgRed, color.FgWhite).Printf(" Invalid number ")
				fmt.Println()
				continue
			}

			account = string(hits[choice])
		} else if ch == 0 && key == keyboard.KeySpace {
			account = prompt.Input("Enter account> ", buildCompleter(p.classes))
			if account == "" {
				return 0
			}
		}

		if account != "" {
			fmt.Println()
			color.New(color.BgWhite, color.FgBlack).Printf("Selected [%s]", account)
			fmt.Println()
			t.setPairAccount(account)
			return 0
		}

		switch ch {
		case 'b':
			return -1
		case 's':
			t.Done = false
			p.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				b.Delete(t.Hash()[:])
				return nil
			})
			return 1
		case 'q':
			return 9999
		case 'a':
			return math.MaxInt16
		}
	}
}

func (p *parser) printTxn(t *txn, idx, total int) int {
	clear()
	printSummary(t, idx, total)
	fmt.Println()
	if len(t.BankDesc) > descLength {
		color.New(color.BgWhite, color.FgBlack).Printf("%6s %s ", "[DESC]", t.BankDesc) // descLength used in Printf.
		fmt.Println()
	}
	{
		prefix, cat := t.getPairAccount2()
		if len(cat) > catLength {
			color.New(color.BgHiYellow, color.FgBlack).Printf("%6s %s", prefix, cat)
			fmt.Println()
		}
		prefix, cat = t.getKnownAccount2()
		if len(cat) > catLength {
			color.New(color.BgGreen, color.FgBlack).Printf("%6s %s", prefix, cat)
			fmt.Println()
		}
	}
	fmt.Println()

	hits := p.topHits(t)
	res := p.printAndGetResult(hits, t)
	if res != math.MaxInt16 {
		return res
	}

	clear()
	printSummary(t, idx, total)
	res = p.printAndGetResult(hits, t)
	return res
}

func (p *parser) showAndCategorizeTxns(txns []*txn) {
	for {
		for i, t := range txns {
			if !t.Done {
				hits := p.topHits(t)
				t.setPairAccount(string(hits[0]))
			}
			printSummary(t, i, len(txns))
		}
		fmt.Println()

		fmt.Printf("Found %d transactions. Review (Y/a/n/q)? ", len(txns))
		ch, _, _ := keyboard.GetSingleKey()
		fmt.Println()

		if ch == 'q' {
			return
		}

		if ch == 'n' || ch == 'a' {
			fmt.Printf("\n\nMarking all transactions as accepted\n\n")
			for i := 0; i < len(txns); i++ {
				txns[i].Done = true
				p.writeToDB(txns[i])
			}

			if ch == 'n' {
				return
			}

			continue
		}

		for i := 0; i < len(txns) && i >= 0; {
			t := txns[i]
			i += p.printTxn(t, i, len(txns))
		}
	}
}

func ledgerFormat(out io.Writer, t *txn) error {
	_, err := fmt.Fprintf(out, "%s %s\n", t.Date.Format(stamp), t.BankDesc)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "\t%-20s \t", t.To)
	if err != nil {
		return err
	}

	if len([]rune(t.Currency)) <= 1 {
		_, err = fmt.Fprintf(out, "%s%.2f\n", t.Currency, math.Abs(t.Amount))
	} else {
		_, err = fmt.Fprintf(out, "%.2f %s\n", math.Abs(t.Amount), t.Currency)
	}
	if err != nil {
		return err
	}
	if t.isToKnown() {
		_, err = fmt.Fprintf(out, "\t; FITID: %s\n", t.FITID)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(out, "\t%s\n", t.From)
	if err != nil {
		return err
	}
	if t.isFromKnown() {
		_, err = fmt.Fprintf(out, "\t; FITID: %s\n", t.FITID)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(out)
	return err
}

func (p *parser) removeDuplicates(tch <-chan *txn) []*txn {
	txns := make([]*txn, 0, 100)
	dupsFound := 0
	for t := range tch {
		hash := t.Hash()
		if !*allowDups && p.knownTxns[*hash] > 0 {
			dupsFound++
			p.knownTxns[*hash]--
			printSummary(t, 0, 0)
		} else {
			txns = append(txns, t)
		}
	}

	fmt.Printf("\t%d FITID duplicates found and ignored.\n", p.skippedFITIDs)
	fmt.Printf("\t%d non-FITID duplicates found and ignored.\n\n", dupsFound)
	return txns
}

func usage() {
	fmt.Println("\nUsage:\n" +
		"\tdirect2ledger [options] <ledger-file>\n\n" +
		"  where:\n" +
		"\t<ledger-file> is an existing Ledger file to learn from\n\n" +
		"Options:")
	flag.PrintDefaults()
	fmt.Println()
	os.Exit(2)
}

func usageMsg(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	usage()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usageMsg("Please specify the input ledger file")
	}

	ledgerFile = flag.Arg(0)

	if len(*output) == 0 {
		usageMsg("Please specify the output file")
	}

	{
		f, err := os.Open("direct2ledger.yaml")
		checkf(err, "Cannot open config file direct2ledger.yaml")
		defer f.Close()

		dec := yaml.NewDecoder(f)
		err = dec.Decode(&config)
		checkf(err, "Cannot read accounts")

		for iBank := range config.Banks {
			bank := &config.Banks[iBank]
			for iAcc := range bank.Accounts {
				acc := &bank.Accounts[iAcc]

				_, alreadyPresent := accountsInfo[acc.Name]
				assertf(!alreadyPresent, "Duplicate account name %s", acc.Name)
				accountsInfo[acc.Name] = &accountInfo{}
			}
		}
	}

	tf := path.Join(os.TempDir(), "ledger-csv-txns")
	defer os.Remove(tf)

	db, err := bolt.Open(tf, 0600, nil)
	checkf(err, "Unable to open boltdb at %v", tf)
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		checkf(err, "Unable to create default bucket in boltdb.")
		return nil
	})

	of, err := os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	checkf(err, "Unable to open output file: %v", *output)

	p := newParser(db)
	p.readAutoAccepts()
	defer p.writeAutoAccepts()
	p.readAccounts()

	tch := make(chan *txn, 100)
	go parseJournalTransactions(tch)

	// Train classifier.
	p.prepareTraining()
	for t := range tch {
		p.train(t)
	}
	p.finishTraining()

	tch = make(chan *txn, 100)
	p.downloadAndParseNewTransactions(tch)

	if !*noBalancesCheck {
		defer checkBalances()
	}

	txns := p.removeDuplicates(tch)
	if len(txns) == 0 {
		return
	}

	sort.Stable(byTime(txns))

	p.showAndCategorizeTxns(txns)

	if !*noTS {
		_, err = fmt.Fprintf(of, "; direct2ledger run at %v\n\n", time.Now().Format("2006-01-02 15:04:05 MST"))
		checkf(err, "Unable to write into output file: %v", of.Name())
	}

	for _, t := range txns {
		if t.Done {
			if err := ledgerFormat(of, t); err != nil {
				log.Fatalf("Unable to write to output: %v", err)
			}
		}
	}
	checkf(of.Close(), "Unable to close output file: %v", of.Name())
}

type amountInCurr struct {
	amount float64
	curr   string
}

func (a *amountInCurr) String() string {
	return fmt.Sprintf("%.2f %s", a.amount, a.curr)
}

func checkBalances() {
	fmt.Println("Checking balances ...")
	ledBals := getLedgerBalances()
	mismatch := false

	table := tablewriter.NewWriter(color.Output)
	table.SetHeader([]string{"Account", "Bank Balance", "Ledger Balance", "Diff"})
	table.SetAutoFormatHeaders(false)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_RIGHT})

	green := color.New(color.BgGreen, color.FgBlack)
	red := color.New(color.BgRed, color.FgWhite)

	for acc, info := range accountsInfo {
		ledBal := ledBals[acc]
		bankBal := info.balance
		if bankBal == ledBal || (bankBal.amount == 0 && ledBal.amount == 0) {
			table.Append([]string{acc, bankBal.String(), ledBal.String(), green.Sprint(" match ")})
		} else {
			table.Append([]string{red.Sprint(acc), bankBal.String(), ledBal.String(), red.Sprintf(" %.2f ", bankBal.amount-ledBal.amount)})
			mismatch = true
		}
	}

	table.Render()

	if !mismatch {
		green.Print(" All balances match! ")
		fmt.Println()
	}
}

func getLedgerBalances() map[string]amountInCurr {
	const errText = "Unable to get accounts balances from journal. Possibly an issue with your ledger installation."

	bals := make(map[string]amountInCurr)
	params := []string{"-f", ledgerFile, "balance", "--flat", "--empty", "--no-total", "--format", `%a\\t%(quantity(amount))\\t%(commodity(amount))\\n`}
	for acc := range accountsInfo {
		params = append(params, fmt.Sprintf("^%s$", acc))
	}
	cmd := exec.Command("ledger", params...)
	out, err := cmd.StdoutPipe()
	checkf(err, errText)

	err = cmd.Start()
	checkf(err, errText)

	s := bufio.NewScanner(out)
	for s.Scan() {
		txt := s.Text()

		entry := strings.Split(txt, "\t")
		assertf(len(entry) == 3, "Cannot parse balance %s", txt)
		acc := entry[0]
		bal, err := strconv.ParseFloat(strings.Replace(entry[1], ",", "", -1), 64)
		checkf(err, "Cannot parse amount in %s", entry[1])
		bals[acc] = amountInCurr{bal, entry[2]}
	}

	// fmt.Printf("Bals:\n%+v\n", bals)

	err = cmd.Wait()
	checkf(err, errText)
	return bals
}

func translateCurrency(cur string) string {
	switch cur {
	case "USD":
		return "$"
	default:
		return cur
	}
}

func formatTid(account, FITID string) string {
	return fmt.Sprintf("%s\n%s", account, FITID)
}

func (p *parser) parseBankTransaction(acc *account, defCurrency ofxgo.CurrSymbol, tran *ofxgo.Transaction) *txn {
	amount, _ := tran.TrnAmt.Float64()
	if amount == 0 {
		// Discover reports some bogus 0-amount txns
		return nil
	}

	if *debug {
		printTransaction(defCurrency, tran)
	}

	tid := formatTid(acc.Name, tran.FiTID.String())

	t := txn{fromJournal: false, FITID: tran.FiTID.String()}

	t.Amount = amount
	assertf(t.Amount != 0.0, "Zero amount for %+v", tran)

	{
		cur := defCurrency
		if _, err := tran.Currency.CurSym.Valid(); err == nil {
			cur = tran.Currency.CurSym
		}
		t.Currency = translateCurrency(cur.String())
	}

	{
		ofxDate := tran.DtPosted
		date := ofxDate.Add(acc.DateAdjustment)
		t.Date = date.UTC().Truncate(24 * time.Hour)
		assertf(!t.Date.IsZero(), "Invalid date for %+v", tran)
	}

	t.BankDesc = extractDescription(tran)
	assertf(len(t.BankDesc) != 0, "No description for %+v", tran)

	t.setKnownAccount(acc.Name)

	// check if it was reconciled before (in case we are restarted after a crash)
	p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		v := b.Get(t.Hash()[:])
		if v != nil {
			dec := gob.NewDecoder(bytes.NewBuffer(v))
			var td txn
			if err := dec.Decode(&td); err == nil {
				t.setPairAccount(td.getPairAccount())
				t.Done = true
			}
		}
		return nil
	})

	if _, known := p.knownTIDs[tid]; known {
		atomic.AddInt32(&p.skippedFITIDs, 1) // Atomic increment
		// printSummary(&t, 0, 0)
		return nil
	}

	return &t
}

func printTransaction(defCurrency ofxgo.CurrSymbol, tran *ofxgo.Transaction) {
	currency := defCurrency
	if ok, _ := tran.Currency.Valid(); ok {
		currency = tran.Currency.CurSym
	}

	var name string
	if len(tran.Name) > 0 {
		name = string(tran.Name)
	} else if tran.Payee != nil {
		name = string(tran.Payee.Name)
	}

	if len(tran.Memo) > 0 {
		name = name + " - " + string(tran.Memo)
	}

	fmt.Printf("%s %-15s %-11s %s\t%s\n", tran.DtPosted, tran.TrnAmt.String()+" "+currency.String(), tran.TrnType, name, tran.FiTID.String())
}
