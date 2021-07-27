package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func add(key, value, ttl string) string {
	url := "http://localhost:8080/add"
	var jsonStr = []byte(fmt.Sprintf("{\"Key\": \"%s\",\"Value\": \"%s\",\"TTL\": \"%s\"}", key, value, ttl))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	return string(body)
}

func find(key string) string {
	url := "http://localhost:8080/find"
	var jsonStr = []byte(fmt.Sprintf("{\"Key\": \"%s\"}", key))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	return string(body)
}

func del(key string) string {
	url := "http://localhost:8080/del"
	var jsonStr = []byte(fmt.Sprintf("{\"Key\": \"%s\"}", key))
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	return string(body)
}

func main() {
	// Изначально требуется проверка работы основных функций;
	// find должен возвращать значение если передданный ключ существует, либо nil если ключа не существует;
	// add должен возвращать true если запись по переденному ключу, значению и TTL прошла успешно
	// неуспешной она может быть если, при записи, хранилище перейдет границу заданного размера
	// в таком случае возвращается false;
	// del удаляет значение по ключу, если ключ успешно найден и удален - возвращается true, если
	// ключа нет в хранилище и соответсвенно удаление выполнить нельзя - возвращаентся false

	fmt.Println(find("Roman"), " - должно выводиться nil, т.к. ключа еще нет")
	fmt.Println(add("Roman", "Niktin", "10s"), " - должно выводиться true, т.к. укладывется в размеры")
	fmt.Println(find("Roman"), " - должно выводиться Nikitin, т.к. такой ключ существует")
	fmt.Println(del("Roman"), " - должно выводиться true, т.к. такой ключ существовал и был удален")
	fmt.Println(del("Roman"), " - должно выводиться false, т.к. такой ключ не существует")
	fmt.Println(find("Roman"), " - должно выводиться nil, т.к. ключа уже нет")

	// По умолчанию (см. метод init файла core.go) работает уборщик, активирующийся каждые 5 секунд,
	// он реализует TTL;
	// Если в TTL передается 0, то этот ключ уборщик удалить не может
	// Так же по умолчанию задано ограничение на размер хранилища в 180 байт;
	// Проверка работы уборщика и ограничение на размер

	fmt.Println(add("Roman", "Niktin", "10s"), "он влезет")
	fmt.Println(add("Ivan", "Ivanov", "30s"), "он влезет")
	fmt.Println(add("Slava", "Petrov", "0"), "он влезет")
	fmt.Println(add("OnNe", "Vlezeeeeeeeeeeeeeeeeeeeeeet", "1h30m10s"), "а он нет, будет уже > 180 байт ")
	t := time.NewTimer(15 * time.Second)
	<-t.C
	// Ожидание уборщика
	fmt.Println(find("Roman"), "прошло 10 сек. - удалился")
	fmt.Println(find("Ivan"), "30 сек. не прошло - остался")
	fmt.Println(find("Slava"), "он вечный")
	fmt.Println(find("OnNe"), "его там и не было")
	t = time.NewTimer(25 * time.Second)
	<-t.C
	fmt.Println(find("Ivan"), "30 сек. прошло - удалился")
	fmt.Println(find("Slava"), "он вечный")

	// Все функции доступны так же из консоли core.go
	// Дополнительно там можно сделать снимок базы в файл и загрузисться с него
	// Итог :
	//* возможность добавить, искать и удалять произвольный набор байт по ключу - ДА
	//* консистентность при параллельных запросах к хранилищу - ДА (горутины для add, find и del, см. core.go)
	//* соблюдение ограничения на размер базы (по объему, задаваемый из конфига) - ДА
	//* тестовый пример: клиент, пишущий и читающий из хранилища - ДА (это он)
	//* сделать бенчмарк (+-, только если этот клиент можно посичтать бенчмарком P.S. с натяжкой)
	//* добавить поддержку TTL для ключей - ДА
	//* добавить поддержку персистентности, чтобы хранилище переживало рестарт - ДА
	//* добавить поддержку типов - ДА (value это interface{})
	//* добавить поддержку репликации - НЕТ (не понял в каком виде она нужна, да и времени не хватило)

}
