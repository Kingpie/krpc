package krpc

import (
	"testing"
)

type User struct {
	Name   string
	Age    int
	Sex    int
	Weight int
}

func (u User) GetInfo() (string, int) {
	return u.Name, u.Age
}

func (u User) SetInfo(name string, age int) error {
	u.Name = name
	u.Age = age
	return nil
}

// 小写
func (u User) setSexAndWeight(sex int, weight int) error {
	u.Sex = sex
	u.Weight = weight
	return nil
}

// 只有SetInfo可以注册成功
func TestService(t *testing.T) {
	newService(User{Name: "tom", Age: 18})
}
