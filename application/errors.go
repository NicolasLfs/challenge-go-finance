package application

import "errors"

var ErrNotFound = errors.New("record not found")

var ErrUniqueConflict = errors.New("unique persistence constraint conflict")

var ErrInboxConflict = errors.New("inbox message ID reused with different content")

var ErrInboxIncomplete = errors.New("inbox message has an incomplete prior delivery")
