package main

import (
	"encoding/json"
	"io/ioutil"
	"keva"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

var store *keva.Store

func main() {
	store = keva.NewStore("2", "127.0.0.1:7777", "", "test_channel", 0, 1000)
	r := mux.NewRouter()
	r.HandleFunc("/keva", kevaGetHandler).Methods(http.MethodGet)
	r.HandleFunc("/keva", kevaPutHandler).Methods(http.MethodPut)

	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8001",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

func kevaGetHandler(w http.ResponseWriter, r *http.Request) {
	queries := r.URL.Query()
	key := queries.Get("key")
	value := store.Get(key)
	var form struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	form.Key = key
	form.Value = value
	bytes, err := json.Marshal(form)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(bytes)
	w.Header().Set("Content-Type", "application/json")
}

func kevaPutHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	defer r.Body.Close()

	var form struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err = json.Unmarshal(body, &form); err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	store.Put(form.Key, form.Value)
	w.Write([]byte("ok"))
}
