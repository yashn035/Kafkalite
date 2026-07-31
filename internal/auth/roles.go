package auth

const (
	RoleAdmin    = "admin"
	RoleProducer = "producer"
	RoleConsumer = "consumer"
)

func CanProduce(role string) bool {
	return role == RoleAdmin || role == RoleProducer
}

func CanConsume(role string) bool {
	return role == RoleAdmin || role == RoleConsumer
}

func CanAdmin(role string) bool {
	return role == RoleAdmin
}
