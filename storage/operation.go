package storage

//////
// Vars, consts, and types.
//////

// Operation is the operation name.
type Operation string

const (
	OperationCount    Operation = "count"
	OperationCreate   Operation = "create"
	OperationDelete   Operation = "delete"
	OperationList     Operation = "list"
	OperationRetrieve Operation = "retrieve"
	OperationUpdate   Operation = "update"
)

//////
// Methods.
//////

// String implements the Stringer interface.
func (o Operation) String() string {
	return string(o)
}
