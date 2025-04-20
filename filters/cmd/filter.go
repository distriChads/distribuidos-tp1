package filters

type Filter interface {
	RunWorker() error
	CloseWorker() error
}
