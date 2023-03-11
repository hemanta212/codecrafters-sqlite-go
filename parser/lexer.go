package parser

import (
	"fmt"
	"log"
	"strings"
	"unicode"
	"unicode/utf8"
)

type itemType int
type Pos int

var eof = rune(0)

const (
	// special tokens
	itemError itemType = iota
	itemEOF
	itemWs // whitespaces

	// Literals
	itemIdent       // fields, table_name
	itemCommand     // sql select, alter
	itemFunctionArg // function arguments Count(a, b)

	// MISC Chars
	itemAsterisk // *
	itemComma    // ,

	// special forms
	itemFunction      // Count(), any itemIdent with ()
	itemFunctionOpen  // '('
	itemFunctionClose // ')'
)

// item represents a token or text string returned from the scanner.
type item struct {
	typ  itemType // The type of this item.
	pos  Pos      // The starting position, in bytes, of this item in the input string.
	val  string   // The value of this item.
	line int      // The line number at the start of this item.
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case len(i.val) > 15:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

type stateFn func(*lexer) stateFn

type lexer struct {
	input     string
	name      string    // the name of the input; used only for error reports
	pos       Pos       // current position in the input
	start     Pos       // start position of this item
	items     chan item // channel of scanned items
	line      int       // 1+number of newlines seen
	startLine int       // start line of this item
	atEOF     bool      // we have hit the end of input and returned eof
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos], l.startLine}
	l.start = l.pos
	l.startLine = l.line
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.atEOF = true
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += Pos(w)
	if r == '\n' {
		l.line++
	}
	return r
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.line += strings.Count(l.input[l.start:l.pos], "\n")
	l.start = l.pos
	l.startLine = l.line
}

// read and skip the next item
func (l *lexer) ignoreNext() {
	l.next()
	l.ignore()
}

func (l *lexer) backup() {
	if !l.atEOF && l.pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
		l.pos -= Pos(w)
		// Correct newline count.
		if r == '\n' {
			l.line--
		}
	}
}

func (l *lexer) peek() rune {
	runeVal := l.next()
	l.backup()
	return runeVal
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.next()) {
		return true
	}
	l.backup()
	return false
}

// consumes all contiguous whitespaces
func (l *lexer) ignoreAllWhitespace() {
	for {
		char := l.peek()
		if isWhitespace(char) {
			l.ignoreNext()
		} else {
			break
		}
	}
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...), l.startLine}
	return nil
}

// nextItem returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) nextItem() item {
	return <-l.items
}

// drain drains the output so the lexing goroutine will exit.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) drain() {
	for range l.items {
	}
}

// lex creates a new scanner for the input string.
func lex(name, input string) *lexer {
	log.Printf("Lexer: lex: Input %q\n", input)
	s := &lexer{
		input:     input,
		name:      name,
		atEOF:     false,
		items:     make(chan item),
		line:      1,
		startLine: 1,
	}
	go s.run()
	return s
}

func (l *lexer) run() {
	for state := lexCommand; state != nil; {
		state = state(l)
		if state == nil {
			log.Println("lex: run: got nil state exiting")
		}
	}
	close(l.items)
}

// State functions

func lexCommand(l *lexer) stateFn {
	for {
		char := l.next()
		log.Printf("Lex: lexCommand: Curr Char %q\n", char)
		if isWhitespace(char) {
			log.Println("Lex: lexCommand: whitespace")
			l.backup()
			if l.pos > l.start {
				log.Printf("Lex: lexCommand: emitting @ %q\n", char)
				l.emit(itemIdent)
				return lexIdent
			}
			l.ignoreNext()
		} else if char == eof {
			return l.errorf("unexpected EOF")
		} else if !isAlphaNumeric(char) {
			return l.errorf("unexpected charectar %#U", char)
		}
	}
}

func lexIdent(l *lexer) stateFn {
	l.ignoreAllWhitespace()
	for {
		char := l.next()
		if isWhitespace(char) {
			l.backup()
			if l.pos > l.start {
				l.emit(itemIdent)
			}
			l.next()
			l.emit(itemWs)
		} else if char == ',' {
			l.backup()
			if l.pos > l.start {
				l.emit(itemIdent)
			}
			l.next()
			l.emit(itemComma)
		} else if char == '*' {
			l.emit(itemAsterisk)
		} else if char == '(' {
			l.backup()
			if l.pos > l.start {
				l.emit(itemIdent)
			}
			l.next()
			l.emit(itemFunctionOpen)
			return lexFunctionArgs // Next state
		} else if char == eof {
			break
		} else if !isAlphaNumeric(char) {
			return l.errorf("unexpected charectar %#U", char)
		}

	}
	// correctly reached EOF.
	if l.pos > l.start {
		l.emit(itemIdent)
	}
	l.emit(itemEOF)
	return nil
}

func lexFunctionArgs(l *lexer) stateFn {
	// ignore space after bracket 'func(  arg1)'
	l.ignoreAllWhitespace()
	for {
		char := l.next()
		if char == ')' {
			l.backup()
			if l.pos > l.start {
				l.emit(itemFunctionArg)
			}
			l.next()
			l.emit(itemFunctionClose)
			return lexIdent
		} else if char == '*' {
			l.emit(itemAsterisk)
		} else if char == ',' {
			l.backup()
			if l.pos > l.start {
				l.emit(itemFunctionArg)
			}
			l.next()
			l.emit(itemComma)
		} else if isWhitespace(char) {
			l.backup()
			if l.pos > l.start {
				l.emit(itemFunctionArg)
			}
			l.ignoreNext()
		} else if char == eof {
			return l.errorf("unexpected eof")
		} else if !isAlphaNumeric(char) {
			return l.errorf("unexpected charectar %#U", char)
		}
	}
}

// isWhiteSpace reports whether r is a space character.
func isWhitespace(r rune) bool {
	log.Printf("Lex: isWhitespace: got rune %q\n", r)
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
