package types

// DbKey represents a key that can be used for database operations
type DbKey interface {
	ToKey() string
}
