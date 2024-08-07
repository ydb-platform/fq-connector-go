package waiter

// Is inteface for every datasource to implement for dbWaiter
type DataSourceRetrierFuncs interface {
	Op() error
	IsRetriableError(error) bool
}
