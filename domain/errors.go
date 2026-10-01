package domain

import "errors"

var (
	ErrInvalidMoney        = errors.New("invalid money")
	ErrNegativeMoney       = errors.New("negative money")
	ErrMoneyOverflow       = errors.New("money overflow")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrInvalidWallet       = errors.New("invalid wallet")
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrInvalidWager        = errors.New("invalid wager transaction")
	ErrInvalidTransition   = errors.New("invalid wager transition")
)
