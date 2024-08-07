package waiter

import "fmt"

var (
	ErrUserNotInitialized  = fmt.Errorf("user is not initialized")
	ErrTableNotInitialized = fmt.Errorf("table is not initialized")
)
