package observation

var _ Storage = (*storageDummyImpl)(nil)

type storageDummyImpl struct {
	Storage
}
