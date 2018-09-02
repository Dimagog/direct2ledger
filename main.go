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
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"os/user"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/eiannone/keyboard"
	"github.com/fatih/color"
	"github.com/jbrukh/bayesian"
	"github.com/manishrjain/keys"
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
	debug        = flag.Bool("debug", false, "Additional debug information if set.")
	journal      = flag.String("j", "", "Existing journal to learn from.")
	output       = flag.String("o", "out.ldg", "Journal file to write to.")
	csvFile      = flag.String("csv", "", "File path of CSV file containing new transactions.")
	currency     = flag.String("c", "", "Set currency if any.")
	dateFormat   = flag.String("d", "1/2/2006", "Express your date format in numeric form w.r.t. Jan 02, 2006, separated by slashes (/). See: https://golang.org/pkg/time/")
	skip         = flag.Int("s", 1, "Number of header lines in CSV to skip")
	configDir    = flag.String("conf", homeDir()+"/.mint2ledger", "Config directory to store various mint2ledger configs in.")
	shortcuts    = flag.String("short", "shortcuts.yaml", "Name of shortcuts file.")
	inverseSign  = flag.Bool("inverseSign", false, "Inverse sign of transaction amounts in CSV.")
	reverseCSV   = flag.Bool("reverseCSV", true, "Reverse order of transactions in CSV")
	allowDups    = flag.Bool("allowDups", false, "Don't filter out duplicate transactions")
	allowPending = flag.Bool("allowPending", false, "Don't filter out pending transactions (pending detection heuristic may not always work)")
	tfidf        = flag.Bool("tfidf", false, "Use TF-IDF classification algorithm instead of Bayesian")

	rtxn   = regexp.MustCompile(`(\d{4}/\d{2}/\d{2})[\W]*(\w.*)`)
	rto    = regexp.MustCompile(`\W*([:\w]+)(.*)`)
	rfrom  = regexp.MustCompile(`\W*([:\w]+).*`)
	rcur   = regexp.MustCompile(`(\d+\.\d+|\d+)`)
	racc   = regexp.MustCompile(`^account[\W]+(.*)`)
	ralias = regexp.MustCompile(`\balias\s(.*)`)

	stamp      = "2006/01/02"
	bucketName = []byte("txns")
	descLength = 40
	catLength  = 20
	short      *keys.Shortcuts

	accMap accountsConfig
)

type accountFlags struct {
	flags map[string]string
}

type configs struct {
	Accounts map[string]map[string]string // account and the corresponding config.
}

type accountsConfig struct {
	Rename map[string]string `yaml:"Rename"`
}

type txn struct {
	Date         time.Time
	BankDesc     string
	MintDesc     string
	MintCategory string
	To           string
	From         string
	Amount       float64
	Currency     string
	Key          []byte
	Done         bool
}

func (t *txn) getKnownAccount() string {
	if t.Amount >= 0 {
		return t.To
	}
	return t.From
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

type byTime []txn

func (b byTime) Len() int               { return len(b) }
func (b byTime) Less(i int, j int) bool { return b[i].Date.Before(b[j].Date) }
func (b byTime) Swap(i int, j int)      { b[i], b[j] = b[j], b[i] }

func checkf(err error, format string, args ...interface{}) {
	if err != nil {
		log.Printf(format, args)
		log.Println()
		log.Fatalf("%+v", errors.WithStack(err))
	}
}

func assertf(ok bool, format string, args ...interface{}) {
	if !ok {
		log.Printf(format, args)
		log.Println()
		log.Fatalf("%+v", errors.Errorf("Should be true, but is false"))
	}
}

func assignForAccount(account string) {
	tree := strings.Split(account, ":")
	assertf(len(tree) > 0, "Expected at least one result. Found none for: %v", account)
	short.AutoAssign(tree[0], "default")
	prev := tree[0]
	for _, c := range tree[1:] {
		if len(c) == 0 {
			continue
		}
		short.AutoAssign(c, prev)
		prev = c
	}
}

type parser struct {
	db       *bolt.DB
	data     []byte
	txns     []txn
	classes  []bayesian.Class
	cl       *bayesian.Classifier
	accounts []string
}

func (p *parser) parseTransactions() {
	out, err := exec.Command("ledger", "-f", *journal, "csv").Output()
	checkf(err, "Unable to convert journal to csv. Possibly an issue with your ledger installation.")
	r := csv.NewReader(newConverter(bytes.NewReader(out)))
	var t txn
	for {
		cols, err := r.Read()
		if err == io.EOF {
			break
		}
		checkf(err, "Unable to read a csv line.")

		t = txn{}
		t.Date, err = time.Parse(stamp, cols[0])
		checkf(err, "Unable to parse time: %v", cols[0])
		t.BankDesc = strings.Trim(cols[2], " \n\t")

		t.To = cols[3]
		assertf(len(t.To) > 0, "Expected TO, found empty.")

		t.Currency = cols[4]
		t.Amount, err = strconv.ParseFloat(cols[5], 64)
		checkf(err, "Unable to parse amount.")

		comment := strings.Split(cols[7], ";")
		if len(comment) > 0 {
			t.MintDesc = normalizeWhitespace(comment[0])
		}
		if len(comment) > 1 {
			t.MintCategory = normalizeWhitespace(comment[1])
		}

		p.txns = append(p.txns, t)

		assignForAccount(t.To)
	}
}

func (p *parser) parseAccounts() {
	s := bufio.NewScanner(bytes.NewReader(p.data))
	var acc string
	for s.Scan() {
		m := racc.FindStringSubmatch(s.Text())
		if len(m) < 2 {
			continue
		}
		acc = m[1]
		if len(acc) == 0 {
			continue
		}
		p.accounts = append(p.accounts, acc)
		assignForAccount(acc)
	}
}

func (p *parser) generateClasses() {
	p.classes = make([]bayesian.Class, 0, 10)
	tomap := make(map[string]bool)
	for _, t := range p.txns {
		tomap[t.To] = true
	}
	for _, a := range p.accounts {
		tomap[a] = true
	}

	for to := range tomap {
		p.classes = append(p.classes, bayesian.Class(to))
	}
	assertf(len(p.classes) > 1, "Expected some categories. Found none.")

	if *tfidf {
		p.cl = bayesian.NewClassifierTfIdf(p.classes...)
	} else {
		p.cl = bayesian.NewClassifier(p.classes...)
	}
	assertf(p.cl != nil, "Expected a valid classifier. Found nil.")
	for _, t := range p.txns {
		if _, has := tomap[t.To]; !has {
			continue
		}
		p.cl.Learn(t.getTerms(), bayesian.Class(t.To))
	}

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

var trimWhitespace = regexp.MustCompile(`^[\s]+|[\s}]+$`)
var dedupWhitespace = regexp.MustCompile(`[\s]{2,}`)

func (t *txn) isFromJournal() bool {
	return t.Key == nil
}

func normalizeWhitespace(s string) string {
	s = trimWhitespace.ReplaceAllString(s, "")
	s = dedupWhitespace.ReplaceAllString(s, " ")
	return s
}

func (t *txn) getTerms() []string {
	desc := strings.ToUpper(t.BankDesc)
	desc = normalizeWhitespace(desc)
	terms := strings.Split(desc, " ")

	terms = append(terms, "BankDesc: "+desc)

	desc = strings.ToUpper(t.MintDesc)
	desc = normalizeWhitespace(desc)
	if desc != "" {
		moreTerms := strings.Split(desc, " ")
		terms = append(terms, moreTerms...)
		terms = append(terms, "MintDesc: "+desc)
	}

	cat := strings.ToUpper(t.MintCategory)
	cat = normalizeWhitespace(cat)
	if cat != "" {
		moreTerms := strings.Split(cat, " ")
		terms = append(terms, moreTerms...)
		terms = append(terms, "MintCategory: "+cat)
	}

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
	result := make([]bayesian.Class, 0, 5)
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

func includeAll(dir string, data []byte) []byte {
	final := make([]byte, len(data))
	copy(final, data)

	b := bytes.NewBuffer(data)
	s := bufio.NewScanner(b)
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "include ") {
			continue
		}
		fname := strings.Trim(line[8:], " \n")
		include, err := ioutil.ReadFile(path.Join(dir, fname))
		checkf(err, "Unable to read file: %v", fname)
		final = append(final, include...)
	}
	return final
}

func parseDate(col string) (time.Time, bool) {
	tm, err := time.Parse(*dateFormat, col)
	if err == nil {
		return tm, true
	}
	return time.Time{}, false
}

func parseAmount(col string) (float64, bool) {
	f, err := strconv.ParseFloat(col, 64)
	return f, err == nil
}

func parseDescription(col string) (string, bool) {
	return strings.Map(func(r rune) rune {
		if r == '"' {
			return -1
		}
		return r
	}, col), true
}

func (p *parser) parseTransactionsFromCSV(in []byte) []txn {
	result := make([]txn, 0, 100)
	r := csv.NewReader(bytes.NewReader(in))
	var t txn
	var skipped int
	for {
		t = txn{}
		cols, err := r.Read()
		if err == io.EOF {
			break
		}
		checkf(err, "Unable to read line: %v", strings.Join(cols, ", "))
		if *skip > skipped {
			skipped++
			continue
		}

		assertf(len(cols) == 9, "Mint export has unexpected number of columns %d", len(cols))

		{
			date, ok := parseDate(cols[0])
			assertf(ok, "Cannot parse date: %s", cols[0])
			y, m, d := date.Year(), date.Month(), date.Day()
			t.Date = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
		}
		assertf(!t.Date.IsZero(), "Invalid date for %v", cols)

		t.MintDesc = cols[1]

		t.BankDesc = dedupDescription(cols[2])
		assertf(len(t.BankDesc) != 0, "No description for %v", cols)

		{
			amt, ok := parseAmount(cols[3])
			assertf(ok, "Cannot parse amount: %s", cols[2])
			if *inverseSign {
				amt = -amt
			}
			if cols[4] == "debit" {
				amt = -amt
			} else {
				assertf(cols[4] == "credit", "Expected debit or credit, got: %s", cols[4])
			}
			t.Amount = amt
		}
		assertf(t.Amount != 0.0, "Zero amount for %v", cols)

		t.MintCategory = cols[5]

		{
			acc := cols[6]
			acc = accMap.Rename[acc]
			assertf(acc != "", "Cannot find mapping for account", cols[6])

			t.setKnownAccount(acc)
		}

		// Have a unique key for each transaction in CSV, so we can uniquely identify and
		// persist them as we modify their category.
		hash := sha256.New()
		fmt.Fprintf(hash, "%s\t%s\t%.2f", t.Date.Format(stamp), t.BankDesc, t.Amount)
		t.Key = hash.Sum(nil)

		// check if it was reconciled before (in case we are restarted after a crash)
		p.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			v := b.Get(t.Key)
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

		result = append(result, t)
	}
	return result
}

func dedupDescription(desc string) string {
outer:
	for j := len(desc) / 2; j > 3; j-- {
		for i := 0; i < j; i++ {
			if desc[i] != desc[j+i] {
				continue outer
			}
		}
		// found the longest duplicate
		if *debug {
			fmt.Printf("Deduped from: %v\n", desc)
			fmt.Printf("Deduped to:   %v\n", desc[j:])
		}
		return desc[j:]
	}
	return desc // no deduping
}

func assignFor(opt string, cl bayesian.Class, keys map[rune]string) bool {
	for i := 0; i < len(opt); i++ {
		ch := rune(opt[i])
		if _, has := keys[ch]; !has {
			keys[ch] = string(cl)
			return true
		}
	}
	return false
}

func setDefaultMappings(ks *keys.Shortcuts) {
	ks.BestEffortAssign('b', ".back", "default")
	ks.BestEffortAssign('q', ".quit", "default")
	ks.BestEffortAssign('a', ".show all", "default")
	ks.BestEffortAssign('s', ".skip", "default")
}

type kv struct {
	key rune
	val string
}

type byVal []kv

func (b byVal) Len() int {
	return len(b)
}

func (b byVal) Less(i int, j int) bool {
	return b[i].val < b[j].val
}

func (b byVal) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

func singleCharMode() {
	// disable input buffering
	exec.Command("stty", "-F", "/dev/tty", "cbreak", "min", "1").Run()
	// do not display entered characters on the screen
	exec.Command("stty", "-F", "/dev/tty", "-echo").Run()
}

func saneMode() {
	exec.Command("stty", "-F", "/dev/tty", "sane").Run()
}

func printCategory(t txn) {
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

func printSummary(t txn, idx, total int) {
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
		fmt.Printf(" hash: %s", hex.EncodeToString(t.Key))
	}
	fmt.Println()
}

func clear() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
	fmt.Println()
}

func (p *parser) writeToDB(t txn) {
	if err := p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		var val bytes.Buffer
		enc := gob.NewEncoder(&val)
		checkf(enc.Encode(t), "Unable to encode txn: %v", t)
		return b.Put(t.Key, val.Bytes())

	}); err != nil {
		log.Fatalf("Write to db failed with error: %v", err)
	}
}

func (p *parser) printAndGetResult(ks keys.Shortcuts, t *txn) int {
	label := "default"

	var repeat bool
	var category []string
LOOP:
	if len(category) > 0 {
		fmt.Println()
		color.New(color.BgWhite, color.FgBlack).Printf("Selected [%s]", strings.Join(category, ":")) // descLength used in Printf.
		fmt.Println()
	}

	ks.Print(label, false)
	ch, key, _ := keyboard.GetSingleKey()
	if ch == 0 && key == keyboard.KeyEnter && len(t.To) > 0 && len(t.From) > 0 {
		t.Done = true
		p.writeToDB(*t)
		if repeat {
			return 0
		}
		return 1
	}

	if opt, has := ks.MapsTo(ch, label); has {
		switch opt {
		case ".back":
			return -1
		case ".skip":
			t.Done = false
			p.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				b.Delete(t.Key)
				return nil
			})
			return 1
		case ".quit":
			return 9999
		case ".show all":
			return math.MaxInt16
		}

		category = append(category, opt)
		t.setPairAccount(strings.Join(category, ":"))
		label = opt
		if ks.HasLabel(label) {
			repeat = true
			goto LOOP
		}
	}
	return 0
}

func (p *parser) printTxn(t *txn, idx, total int) int {
	clear()
	printSummary(*t, idx, total)
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
	var ks keys.Shortcuts
	setDefaultMappings(&ks)
	for _, hit := range hits {
		ks.AutoAssign(string(hit), "default")
	}
	res := p.printAndGetResult(ks, t)
	if res != math.MaxInt16 {
		return res
	}

	clear()
	printSummary(*t, idx, total)
	res = p.printAndGetResult(*short, t)
	return res
}

func (p *parser) showAndCategorizeTxns(rtxns []txn) {
	txns := rtxns
	for {
		for i := 0; i < len(txns); i++ {
			// for i := range txns {
			t := &txns[i]
			if !t.Done {
				hits := p.topHits(t)
				t.setPairAccount(string(hits[0]))
			}
			printSummary(*t, i, len(txns))
		}
		fmt.Println()

		fmt.Printf("Found %d transactions. Review (Y/a/n/q)? ", len(txns))
		ch, _, _ := keyboard.GetSingleKey()

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
			t := &txns[i]
			i += p.printTxn(t, i, len(txns))
		}
	}
}

func ledgerFormat(t txn) string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("%s %s\n", t.Date.Format(stamp), t.BankDesc))
	if t.MintDesc != "" || t.MintCategory != "" {
		b.WriteString(fmt.Sprintf("\t; %s ; %s\n", t.MintDesc, t.MintCategory))
	}
	b.WriteString(fmt.Sprintf("\t%-20s\t%.2f%s\n", t.To, math.Abs(t.Amount), t.Currency))
	b.WriteString(fmt.Sprintf("\t%s\n\n", t.From))
	return b.String()
}

func sanitize(a string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r
		}
		if r >= '0' && r <= '9' {
			return r
		}
		switch r {
		case '*':
			fallthrough
		case ':':
			fallthrough
		case '/':
			fallthrough
		case '.':
			fallthrough
		case '-':
			return r
		default:
			return -1
		}
	}, a)
}

func (p *parser) removeDuplicates(txns []txn) []txn {
	if len(txns) == 0 {
		return txns
	}

	sort.Stable(byTime(txns))
	if *allowDups {
		return txns
	}

	sort.Sort(byTime(p.txns))

	prev := p.txns
	first := txns[0].Date.Add(-24 * time.Hour)
	for i, t := range p.txns {
		if t.Date.After(first) {
			prev = p.txns[i:]
			break
		}
	}

	final := txns[:0]
	for _, t := range txns {
		var found bool
		tdesc := sanitize(t.BankDesc)
		for _, pr := range prev {
			if pr.Date.After(t.Date) {
				break
			}
			pdesc := sanitize(pr.BankDesc)
			if tdesc == pdesc && pr.Date.Equal(t.Date) && math.Abs(pr.Amount) == math.Abs(t.Amount) {
				printSummary(t, 0, 0)
				found = true
				break
			}
		}
		if !found {
			final = append(final, t)
		}
	}
	fmt.Printf("\t%d duplicates found and ignored.\n\n", len(txns)-len(final))
	return final
}

var errc = color.New(color.BgRed, color.FgWhite).PrintfFunc()

func oerr(msg string) {
	errc("\tERROR: " + msg + " ")
	fmt.Println()
	fmt.Println("Flags available:")
	flag.PrintDefaults()
	fmt.Println()
}

func reverseSlice(s []txn) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func main() {
	flag.Parse()

	if len(*journal) == 0 {
		oerr("Please specify the input ledger journal file")
		return
	}

	if len(*output) == 0 {
		oerr("Please specify the output file")
		return
	}

	defer saneMode()
	singleCharMode()

	checkf(os.MkdirAll(*configDir, 0755), "Unable to create directory: %v", *configDir)

	{
		f, err := os.Open("mint2ledger.yaml")
		checkf(err, "Cannot open config file mint2ledger.yaml")
		defer f.Close()

		dec := yaml.NewDecoder(f)
		err = dec.Decode(&accMap)
		checkf(err, "Cannot decode accounts map")
	}

	keyfile := path.Join(*configDir, *shortcuts)
	short = keys.ParseConfig(keyfile)
	setDefaultMappings(short)
	defer short.Persist(keyfile)

	data, err := ioutil.ReadFile(*journal)
	checkf(err, "Unable to read file: %v", *journal)
	alldata := includeAll(path.Dir(*journal), data)

	if _, err := os.Stat(*output); os.IsNotExist(err) {
		_, err := os.Create(*output)
		checkf(err, "Unable to check for output file: %v", *output)
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

	of, err := os.OpenFile(*output, os.O_APPEND|os.O_WRONLY, 0600)
	checkf(err, "Unable to open output file: %v", *output)

	p := parser{data: alldata, db: db}
	p.parseAccounts()
	p.parseTransactions()

	// Scanning done. Now train classifier.
	p.generateClasses()

	in, err := ioutil.ReadFile(*csvFile)
	checkf(err, "Unable to read csv file: %v", *csvFile)
	txns := p.parseTransactionsFromCSV(in)
	txns = removePendingTransactions(txns)
	if *reverseCSV {
		reverseSlice(txns)
	}

	for i := range txns {
		txns[i].Currency = *currency
	}

	txns = p.removeDuplicates(txns)
	if len(txns) == 0 {
		return
	}

	p.showAndCategorizeTxns(txns)

	_, err = of.WriteString(fmt.Sprintf("; mint2ledger run at %v\n\n", time.Now().Format("2006-01-02 15:04:05 MST")))
	checkf(err, "Unable to write into output file: %v", of.Name())

	for _, t := range txns {
		if t.Done {
			if _, err := of.WriteString(ledgerFormat(t)); err != nil {
				log.Fatalf("Unable to write to output: %v", err)
			}
		}
	}
	checkf(of.Close(), "Unable to close output file: %v", of.Name())
}

func removePendingTransactions(txns []txn) []txn {
	if !*allowPending && len(txns) > 0 {
		lastDate := txns[0].Date
		for i, t := range txns {
			if t.Date.After(lastDate) {
				for j := 0; j < i; j++ {
					printSummary(txns[j], 0, -1)
				}
				fmt.Printf("\t%d pending transactions were ignored.\n\n", i)
				return txns[i:]
			}
			lastDate = t.Date
		}
	}
	return txns
}
