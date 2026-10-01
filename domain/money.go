package domain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const moneyScale int64 = 100

// Money stores the amount as signed minor units. External input remains non-negative.
type Money struct {
	minorUnits int64
	currency   string
}

func ParseMoney(value, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}
	if value == "" || strings.TrimSpace(value) != value {
		return Money{}, ErrInvalidMoney
	}
	if strings.HasPrefix(value, "-") {
		return Money{}, ErrNegativeMoney
	}
	if value == "" || strings.Count(value, ".") > 1 {
		return Money{}, ErrInvalidMoney
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 2 {
		return Money{}, ErrInvalidMoney
	}
	if !allDigits(parts[0]) || !allDigits(parts[1]) {
		return Money{}, ErrInvalidMoney
	}
	fraction := parts[1]
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || whole > uint64(math.MaxInt64/moneyScale) {
		return Money{}, ErrMoneyOverflow
	}
	minor := whole * uint64(moneyScale)
	decimal, err := strconv.ParseUint(fraction, 10, 64)
	if err != nil || minor > uint64(math.MaxInt64)-decimal {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: int64(minor + decimal), currency: currency}, nil
}

func MoneyFromMinorUnits(minorUnits int64, currency string) (Money, error) {
	if err := validateCurrency(currency); err != nil {
		return Money{}, err
	}
	return Money{minorUnits: minorUnits, currency: currency}, nil
}

func ZeroMoney(currency string) (Money, error) {
	return MoneyFromMinorUnits(0, currency)
}

func (money Money) MinorUnits() int64 { return money.minorUnits }
func (money Money) Currency() string  { return money.currency }

func (money Money) Decimal() string {
	if money.minorUnits < 0 {
		if money.minorUnits == math.MinInt64 {
			return "-92233720368547758.08"
		}
		return "-" + formatMinorUnits(-money.minorUnits)
	}
	return formatMinorUnits(money.minorUnits)
}

func (money Money) String() string { return money.Decimal() + " " + money.currency }

func (money Money) Add(other Money) (Money, error) {
	if err := money.compatible(other); err != nil {
		return Money{}, err
	}
	if (other.minorUnits > 0 && money.minorUnits > math.MaxInt64-other.minorUnits) || (other.minorUnits < 0 && money.minorUnits < math.MinInt64-other.minorUnits) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: money.minorUnits + other.minorUnits, currency: money.currency}, nil
}

func (money Money) Subtract(other Money) (Money, error) {
	if err := money.compatible(other); err != nil {
		return Money{}, err
	}
	if other.minorUnits == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return money.Add(Money{minorUnits: -other.minorUnits, currency: other.currency})
}

func (money Money) Negate() (Money, error) {
	if money.minorUnits == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: -money.minorUnits, currency: money.currency}, nil
}

func (money Money) Equal(other Money) bool {
	return money.currency == other.currency && money.minorUnits == other.minorUnits
}
func (money Money) GreaterThan(other Money) bool {
	return money.compatible(other) == nil && money.minorUnits > other.minorUnits
}
func (money Money) LessThan(other Money) bool {
	return money.compatible(other) == nil && money.minorUnits < other.minorUnits
}

func (money Money) compatible(other Money) error {
	if money.currency == "" || other.currency == "" || money.currency != other.currency {
		return fmt.Errorf("%w: %s and %s", ErrCurrencyMismatch, money.currency, other.currency)
	}
	return nil
}

func validateCurrency(currency string) error {
	if len(currency) != 3 || strings.ToUpper(currency) != currency || !allLetters(currency) {
		return fmt.Errorf("%w: invalid currency %q", ErrInvalidMoney, currency)
	}
	if _, ok := iso4217Currencies[currency]; !ok {
		return fmt.Errorf("%w: invalid currency %q", ErrInvalidMoney, currency)
	}
	return nil
}

var iso4217Currencies = map[string]struct{}{
	"AED": {}, "AFN": {}, "ALL": {}, "AMD": {}, "ANG": {}, "AOA": {}, "ARS": {}, "AUD": {}, "AWG": {}, "AZN": {},
	"BAM": {}, "BBD": {}, "BDT": {}, "BGN": {}, "BHD": {}, "BIF": {}, "BMD": {}, "BND": {}, "BOB": {}, "BOV": {},
	"BRL": {}, "BSD": {}, "BTN": {}, "BWP": {}, "BYN": {}, "BZD": {}, "CAD": {}, "CDF": {}, "CHE": {}, "CHF": {},
	"CHW": {}, "CLF": {}, "CLP": {}, "CNY": {}, "COP": {}, "COU": {}, "CRC": {}, "CUC": {}, "CUP": {}, "CVE": {},
	"CZK": {}, "DJF": {}, "DKK": {}, "DOP": {}, "DZD": {}, "EGP": {}, "ERN": {}, "ETB": {}, "EUR": {}, "FJD": {},
	"FKP": {}, "GBP": {}, "GEL": {}, "GHS": {}, "GIP": {}, "GMD": {}, "GNF": {}, "GTQ": {}, "GYD": {}, "HKD": {},
	"HNL": {}, "HTG": {}, "HUF": {}, "IDR": {}, "ILS": {}, "INR": {}, "IQD": {}, "IRR": {}, "ISK": {}, "JMD": {},
	"JOD": {}, "JPY": {}, "KES": {}, "KGS": {}, "KHR": {}, "KMF": {}, "KPW": {}, "KRW": {}, "KWD": {}, "KYD": {},
	"KZT": {}, "LAK": {}, "LBP": {}, "LKR": {}, "LRD": {}, "LSL": {}, "LYD": {}, "MAD": {}, "MDL": {}, "MGA": {},
	"MKD": {}, "MMK": {}, "MNT": {}, "MOP": {}, "MRU": {}, "MUR": {}, "MVR": {}, "MWK": {}, "MXN": {}, "MXV": {},
	"MYR": {}, "MZN": {}, "NAD": {}, "NGN": {}, "NIO": {}, "NOK": {}, "NPR": {}, "NZD": {}, "OMR": {}, "PAB": {},
	"PEN": {}, "PGK": {}, "PHP": {}, "PKR": {}, "PLN": {}, "PYG": {}, "QAR": {}, "RON": {}, "RSD": {}, "RUB": {},
	"RWF": {}, "SAR": {}, "SBD": {}, "SCR": {}, "SDG": {}, "SEK": {}, "SGD": {}, "SHP": {}, "SLE": {}, "SLL": {},
	"SOS": {}, "SRD": {}, "SSP": {}, "STN": {}, "SVC": {}, "SYP": {}, "SZL": {}, "THB": {}, "TJS": {}, "TMT": {},
	"TND": {}, "TOP": {}, "TRY": {}, "TTD": {}, "TWD": {}, "TZS": {}, "UAH": {}, "UGX": {}, "USD": {}, "USN": {},
	"UYI": {}, "UYU": {}, "UYW": {}, "UZS": {}, "VED": {}, "VES": {}, "VND": {}, "VUV": {}, "WST": {}, "XAF": {},
	"XAG": {}, "XAU": {}, "XBA": {}, "XBB": {}, "XBC": {}, "XBD": {}, "XCD": {}, "XDR": {}, "XOF": {}, "XPD": {},
	"XPF": {}, "XPT": {}, "XSU": {}, "XTS": {}, "XUA": {}, "XXX": {}, "YER": {}, "ZAR": {}, "ZMW": {}, "ZWL": {},
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func allLetters(value string) bool {
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func formatMinorUnits(minorUnits int64) string {
	return strconv.FormatInt(minorUnits/moneyScale, 10) + "." + fmt.Sprintf("%02d", minorUnits%moneyScale)
}
