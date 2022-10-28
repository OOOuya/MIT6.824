package main

import (
	"log"
	"os"
)

func main() {
	filename := "main/mr-tmp/mr-0-2"
	getwd, _ := os.Getwd()
	log.Print(getwd)
	_, err := os.Open(filename)
	if err != nil {
		log.Fatal("intermediate file cannot be opened!")
	}
}
