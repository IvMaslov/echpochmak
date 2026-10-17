package memstore_test

import (
	"testing"

	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
	"echpochmak/internal/store/storetest"
)

func TestContract(t *testing.T) {
	t.Parallel()

	storetest.Run(t, func(*testing.T) store.Store {
		return memstore.New()
	})
}
