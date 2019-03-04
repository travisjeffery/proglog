package main

import (
	"log"
	 "net/http"

	"github.com/gorilla/mux"
	proghttp "github.com/travisjeffery/proglog/http"
)

func main() {
	srv := proghttp.NewServer()
	r := mux.NewRouter()
	r.HandleFunc("/", srv.Produce).Methods("POST")
	r.HandleFunc("/", srv.Consume).Methods("GET")
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
