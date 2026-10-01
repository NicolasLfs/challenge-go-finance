package domain

import (
	"errors"
	"math"
	"testing"
)

func MustMoney(value, currency string) Money {
	money, err := ParseMoney(value, currency)
	if err != nil {
		panic(err)
	}
	return money
}

func TestParseMoney(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr error
	}{
		{name: "fixed scale", input: "25.00", want: 2500},
		{name: "one fractional digit", input: "25.5", wantErr: ErrInvalidMoney},
		{name: "leading plus", input: "+25.00", wantErr: ErrInvalidMoney},
		{name: "empty", input: "", wantErr: ErrInvalidMoney},
		{name: "negative external value", input: "-1.00", wantErr: ErrNegativeMoney},
		{name: "too many decimals", input: "1.001", wantErr: ErrInvalidMoney},
		{name: "scientific notation", input: "1e2", wantErr: ErrInvalidMoney},
		{name: "non finite", input: "NaN", wantErr: ErrInvalidMoney},
		{name: "overflow", input: "92233720368547758.08", wantErr: ErrMoneyOverflow},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			money, err := ParseMoney(test.input, "BRL")
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ParseMoney() error = %v, want %v", err, test.wantErr)
			}
			if err == nil && (money.MinorUnits() != test.want || money.Currency() != "BRL") {
				t.Fatalf("ParseMoney() = %d %s, want %d BRL", money.MinorUnits(), money.Currency(), test.want)
			}
		})
	}
}

func TestParseMoneyRejectsUnknownCurrency(t *testing.T) {
	if _, err := ParseMoney("1.00", "ZZZ"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("unknown currency error = %v", err)
	}
}

func TestMoneyArithmeticAndSerialization(t *testing.T) {
	left := MustMoney("10.25", "BRL")
	right := MustMoney("2.00", "BRL")

	if got, err := left.Add(right); err != nil || got.String() != "12.25 BRL" {
		t.Fatalf("Add() = %v, %v", got, err)
	}
	if got, err := left.Subtract(right); err != nil || got.String() != "8.25 BRL" {
		t.Fatalf("Subtract() = %v, %v", got, err)
	}
	if got, err := left.Negate(); err != nil || got.MinorUnits() != -1025 {
		t.Fatalf("Negate() = %v, %v", got, err)
	}
	if got := left.Decimal(); got != "10.25" {
		t.Fatalf("Decimal() = %q", got)
	}
	if !left.GreaterThan(right) || !right.LessThan(left) || !left.Equal(left) {
		t.Fatal("comparison result is incorrect")
	}
	if _, err := left.Add(MustMoney("1.00", "USD")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch error = %v", err)
	}
	minimum, err := MoneyFromMinorUnits(math.MinInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MustMoney("0.01", "BRL").Subtract(minimum); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("subtraction overflow error = %v", err)
	}
	if _, err := MustMoney("92233720368547758.07", "BRL").Add(MustMoney("0.01", "BRL")); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("addition overflow error = %v", err)
	}
	minimum, minimumErr := MoneyFromMinorUnits(math.MinInt64, "BRL")
	if minimumErr != nil {
		t.Fatal(minimumErr)
	}
	if _, negateErr := minimum.Negate(); !errors.Is(negateErr, ErrMoneyOverflow) {
		t.Fatalf("negation overflow error = %v", negateErr)
	}
}

func TestZeroMoney(t *testing.T) {
	zero, err := ZeroMoney("BRL")
	if err != nil || zero.Decimal() != "0.00" {
		t.Fatalf("ZeroMoney() = %v, %v", zero, err)
	}
}
