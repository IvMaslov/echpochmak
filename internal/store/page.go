package store

import (
	"encoding/base64"
	"fmt"
	"sort"
)

// Page size limits applied by [PageRequest.Limit].
const (
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

// PageRequest asks for one page of a sorted listing.
type PageRequest struct {
	// Token is the NextToken of the previous page, or "" for the first page.
	Token string
	// Size is the maximum number of items to return. Zero means
	// [DefaultPageSize]; larger values are capped at [MaxPageSize].
	Size int
}

// Limit returns the effective page size of req.
func (req PageRequest) Limit() int {
	switch {
	case req.Size <= 0:
		return DefaultPageSize
	case req.Size > MaxPageSize:
		return MaxPageSize
	default:
		return req.Size
	}
}

// Page is one page of a sorted listing.
type Page[T any] struct {
	// Items is never nil.
	Items []T
	// NextToken continues the listing, or is "" when there are no more items.
	NextToken string
}

// EncodePageToken returns the opaque token for a listing that continues after
// the item with sort key lastKey.
//
// Tokens hold the last returned key rather than an offset, so a listing stays
// stable when items are inserted or removed between pages.
func EncodePageToken(lastKey string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(lastKey))
}

// DecodePageToken returns the sort key held by token. An empty token decodes
// to "", which sorts before every valid key.
func DecodePageToken(token string) (string, error) {
	key, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("%w: malformed page token", ErrInvalidArgument)
	}

	return string(key), nil
}

// Paginate returns the page of items described by req. Items must be sorted by
// key in ascending order, and keys must be unique.
func Paginate[T any](items []T, key func(T) string, req PageRequest) (Page[T], error) {
	after, err := DecodePageToken(req.Token)
	if err != nil {
		return Page[T]{}, err
	}

	start := 0
	if req.Token != "" {
		start = sort.Search(len(items), func(i int) bool {
			return key(items[i]) > after
		})
	}

	end := min(start+req.Limit(), len(items))

	page := Page[T]{
		Items: make([]T, 0, end-start),
	}
	page.Items = append(page.Items, items[start:end]...)

	if end < len(items) {
		page.NextToken = EncodePageToken(key(items[end-1]))
	}

	return page, nil
}
